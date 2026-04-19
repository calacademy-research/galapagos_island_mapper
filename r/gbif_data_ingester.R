# =========================================================
# GBIF Multi-Pull Ingester and Deduplicator
# =========================================================
# Reads one or more GBIF occurrence TSV downloads, combines
# them into a single deduplicated dataset, and writes the
# best version of each unique record.
#
# Workflow:
#   1. List your GBIF download files in the 'pulls' table
#   2. Run sections 1-8 to produce deduplicated output
#   3. Run analyze.py on the output (see Section 9)
#   4. Run section 10 to merge island assignments back in
#   5. Run species_by_island.r to create species lists for 
#       each island
# =========================================================

library(dplyr)
library(stringr)
library(purrr)
library(readr)
library(tidyr)
library(data.table)

# =========================================================
# SECTION 1: INPUT FILES
# =========================================================

pulls <- tibble::tribble(
  ~source_file,                                                                          ~pull_date,    ~download_doi,
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_gbif_12APR2026.txt",       "2026-04-14",  "https://doi.org/10.15468/dl.4r5r7j",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_gbif_24SEP2025.txt",       "2025-09-24",  "https://doi.org/10.15468/dl.hzhmgr",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_28JAN2024.txt",            "2024-01-28",  "https://doi.org/10.15468/dl.3zpgd8",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_15APR26.txt",              "2026-04-15",  "10.15468/dl.9xv8jq",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_0025802-260409193756587.txt",              "2026-04-17",  "10.15468/dl.3jeq3p
",
) %>%
  mutate(pull_date = as.Date(pull_date))

OUTPUT_DIR  <- "~/Dropbox/Galapagos_data/output/"
RESULTS_TSV <- "~/galapagos_island_mapper/results.tsv"

# =========================================================
# SECTION 2: HELPER FUNCTIONS
# =========================================================

# Convert blank strings / "NA" / "NULL" to NA and trim whitespace
clean_characters <- function(df) {
  df %>%
    mutate(across(
      where(is.character),
      ~ .x %>% str_trim() %>% na_if("") %>% na_if("NA") %>% na_if("NULL")
    ))
}

# Standardize a text field: lowercase, trim, collapse internal whitespace
std_text <- function(x) {
  x %>% str_trim() %>% str_squish() %>% str_to_lower()
}

# Safe column getter: returns NA vector if column is absent from df
get_col <- function(df, col) {
  if (col %in% names(df)) df[[col]] else rep(NA_character_, nrow(df))
}

# Count non-missing fields per row (higher = richer record)
score_record_richness <- function(df) {
  fields_to_score <- c(
    "occurrenceID", "catalogNumber", "institutionCode", "collectionCode",
    "datasetKey", "scientificName", "species", "acceptedScientificName",
    "decimalLatitude", "decimalLongitude", "countryCode", "stateProvince",
    "island", "locality", "eventDate", "year", "month", "day",
    "recordedBy", "identifiedBy", "basisOfRecord"
  )
  present_fields <- intersect(fields_to_score, names(df))
  if (length(present_fields) == 0) return(rep(0L, nrow(df)))
  df %>%
    transmute(across(all_of(present_fields), ~ !is.na(.x) & .x != "")) %>%
    mutate(richness_score = rowSums(across(everything()))) %>%
    pull(richness_score)
}

# Read one GBIF TSV file and append provenance columns
read_gbif_pull <- function(source_file, pull_date, download_doi) {
  readr::read_tsv(
    source_file,
    col_types      = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress       = FALSE
  ) %>%
    mutate(
      source_file   = source_file,
      pull_date     = as.Date(pull_date),
      download_doi  = download_doi
    )
}

# Replace embedded tabs and newlines in all character columns with a space.
# This is essential before writing a TSV that will be read by external tools
# (e.g. Python/pandas) that do not handle quoted fields: write_tsv wraps
# fields containing tabs in double quotes, which then look like extra columns
# to any reader using QUOTE_NONE mode.
clean_delimiters <- function(df) {
  df %>%
    mutate(across(where(is.character), ~ str_replace_all(.x, "[\t\n\r]", " ")))
}

# =========================================================
# SECTION 3: READ AND COMBINE ALL PULLS
# =========================================================

cat("Reading", nrow(pulls), "GBIF pull files...\n")

all_pulls <- purrr::pmap(
  pulls,
  function(source_file, pull_date, download_doi) {
    cat(" ", basename(source_file), "... ")
    df <- read_gbif_pull(source_file, pull_date, download_doi)
    cat(nrow(df), "rows,", ncol(df), "columns\n")
    df
  }
)

# Report column differences between files — helpful for spotting
# schema changes across GBIF download versions
all_col_sets <- map(all_pulls, names)
union_cols   <- Reduce(union, all_col_sets)
intersect_cols <- Reduce(intersect, all_col_sets)
cols_only_in_some <- setdiff(union_cols, intersect_cols)

if (length(cols_only_in_some) > 0) {
  cat("\nColumns not present in all files (will be NA where missing):\n")
  for (col in sort(cols_only_in_some)) {
    present_in <- which(map_lgl(all_col_sets, ~ col %in% .x))
    cat(sprintf("  %-40s present in file(s): %s\n", col,
                paste(present_in, collapse = ", ")))
  }
} else {
  cat("\nAll files have identical column sets.\n")
}

# bind_rows fills missing columns with NA — no data loss
gbif_archive <- bind_rows(all_pulls) %>%
  clean_characters()

cat(sprintf("\nCombined: %d rows, %d columns\n\n", nrow(gbif_archive), ncol(gbif_archive)))

# =========================================================
# SECTION 4: STANDARDIZE KEY FIELDS
# =========================================================

gbif_archive_std <- gbif_archive %>%
  mutate(
    occurrenceID_std              = std_text(get_col(., "occurrenceID")),
    catalogNumber_std             = std_text(get_col(., "catalogNumber")),
    institutionCode_std           = std_text(get_col(., "institutionCode")),
    collectionCode_std            = std_text(get_col(., "collectionCode")),
    publisher_std                 = std_text(get_col(., "publisher")),
    datasetKey_std                = std_text(get_col(., "datasetKey")),
    scientificName_std            = std_text(get_col(., "scientificName")),
    species_std                   = std_text(get_col(., "species")),
    acceptedScientificName_std    = std_text(get_col(., "acceptedScientificName")),
    recordedBy_std                = std_text(get_col(., "recordedBy")),
    eventDate_std                 = std_text(get_col(., "eventDate")),
    locality_std                  = std_text(get_col(., "locality")),
    island_std                    = std_text(get_col(., "island")),
    decimalLatitude_std           = suppressWarnings(as.numeric(get_col(., "decimalLatitude"))),
    decimalLongitude_std          = suppressWarnings(as.numeric(get_col(., "decimalLongitude")))
  )

# =========================================================
# SECTION 5: DEDUPLICATION KEY
# =========================================================
# Priority:
#   1. occurrenceID                              (globally unique)
#   2. institutionCode + collectionCode + catalogNumber
#   3. datasetKey + catalogNumber
#   4. fallback composite (taxon + date + place + collector + coords)

gbif_archive_std <- gbif_archive_std %>%
  mutate(
    taxon_std = coalesce(acceptedScientificName_std, species_std, scientificName_std),
    place_std = coalesce(island_std, locality_std),
    dedup_key = case_when(
      !is.na(occurrenceID_std) ~
        paste0("occ:", occurrenceID_std),

      !is.na(institutionCode_std) & !is.na(collectionCode_std) & !is.na(catalogNumber_std) ~
        paste("icc", institutionCode_std, collectionCode_std, catalogNumber_std, sep = "||"),

      !is.na(datasetKey_std) & !is.na(catalogNumber_std) ~
        paste("dscat", datasetKey_std, catalogNumber_std, sep = "||"),

      TRUE ~ paste(
        "fallback",
        coalesce(taxon_std,              "na"),
        coalesce(eventDate_std,          "na"),
        coalesce(recordedBy_std,         "na"),
        coalesce(place_std,              "na"),
        coalesce(as.character(decimalLatitude_std),  "na"),
        coalesce(as.character(decimalLongitude_std), "na"),
        sep = "||"
      )
    ),
    dedup_key_type = case_when(
      str_starts(dedup_key, "occ:")    ~ "occurrenceID",
      str_starts(dedup_key, "icc||")  ~ "institution+collection+catalog",
      str_starts(dedup_key, "dscat||") ~ "datasetKey+catalogNumber",
      TRUE                             ~ "fallback_composite"
    )
  )

cat("Deduplication key type breakdown:\n")
gbif_archive_std %>% count(dedup_key_type, sort = TRUE) %>% print()
cat("\n")

# =========================================================
# SECTION 6: SCORE AND PICK BEST VERSION PER RECORD
# =========================================================

gbif_archive_std <- gbif_archive_std %>%
  mutate(richness_score = score_record_richness(.))

# For each dedup_key: keep highest richness_score;
# break ties by most recent pull_date; then first row encountered.
gbif_current_best <- gbif_archive_std %>%
  group_by(dedup_key) %>%
  arrange(desc(richness_score), desc(pull_date), .by_group = TRUE) %>%
  slice(1) %>%
  ungroup()

# Keep only rows with a recognised basisOfRecord
# (filters out GBIF archive metadata rows that appear as data rows)
gbif_current_best <- gbif_current_best %>%
  filter(basisOfRecord %in% c(
    "HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "PRESERVED_SPECIMEN",
    "FOSSIL_SPECIMEN",   "MATERIAL_SAMPLE",     "MATERIAL_CITATION",
    "LIVING_SPECIMEN",   "LITERATURE",           "OCCURRENCE"
  ))

cat(sprintf("After deduplication: %d unique records\n\n", nrow(gbif_current_best)))

# =========================================================
# SECTION 7: DUPLICATE HISTORY SUMMARY
# =========================================================

gbif_duplicate_history <- gbif_archive_std %>%
  group_by(dedup_key) %>%
  summarise(
    n_versions       = n(),
    first_pull_date  = min(pull_date, na.rm = TRUE),
    last_pull_date   = max(pull_date, na.rm = TRUE),
    key_type         = first(dedup_key_type),
    source_files     = paste(unique(source_file), collapse = "; "),
    download_dois    = paste(unique(download_doi), collapse = "; "),
    .groups = "drop"
  )

# =========================================================
# SECTION 8: WRITE OUTPUTS
# =========================================================

# Full archive (all versions of every record) — R-readable only
write_tsv(gbif_archive_std,       file.path(OUTPUT_DIR, "gbif_archive_all_versions.tsv"))

# Duplicate history summary
write_tsv(gbif_duplicate_history, file.path(OUTPUT_DIR, "gbif_duplicate_history_summary.tsv"))

# Best deduplicated records — fed to analyze.py next.
# Must be written WITHOUT quoting so that Python/pandas can read it
# with QUOTE_NONE mode.  clean_delimiters() replaces any embedded
# tab/newline characters in text fields with spaces before writing,
# ensuring every row has exactly the right number of fields.
gbif_current_best %>%
  clean_delimiters() %>%
  write.table(
    file.path(OUTPUT_DIR, "gbif_occurrences_deduplicated_best.tsv"),
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE,
    na        = ""
  )

cat("Written outputs to:", OUTPUT_DIR, "\n")
cat("Next step: bash analyze.sh",
    file.path(OUTPUT_DIR, "gbif_occurrences_deduplicated_best.tsv"), "\n\n")

# =========================================================
# SECTION 9: RUN analyze.py  (terminal command)
# =========================================================
#
#   cd /Users/jdumbacher/galapagos_island_mapper
#   bash analyze.sh ~/Dropbox/Galapagos_data/output/gbif_occurrences_deduplicated_best.tsv

# =========================================================
# SECTION 10: MERGE ISLAND ASSIGNMENTS
# =========================================================

# Load results produced by analyze.py
results <- fread(RESULTS_TSV) %>%
  clean_characters() %>%
  mutate(across(everything(), as.character)) %>%
  select(gbifID, best, name, latlon)

# Join island assignments onto the deduplicated records
galapagos_data <- inner_join(gbif_current_best, results, by = "gbifID")

# Sanity check: records in best that have no analyze.py result (should be 0)
missing_results <- anti_join(gbif_current_best, results, by = "gbifID")
if (nrow(missing_results) > 0) {
  warning(sprintf(
    "%d records have no match in results.tsv — re-run analyze.py?",
    nrow(missing_results)
  ))
}

cat(sprintf("galapagos_data ready: %d records\n", nrow(galapagos_data)))

# =========================================================
# SECTION 11: filter to Galapagos specimens
# =========================================================


# Matches all common spellings: Galapagos, Galápagos, Galpagos, etc.
GALAPAGOS_PATTERN <- regex("gal[aá]?pag", ignore_case = TRUE)

n_resolved <- sum(ecuador_data_merged$best != "-" & !is.na(ecuador_data_merged$best))

galapagos_vertebrate_specimens <- ecuador_data_merged %>%
  
  # Step 1: keep only records that analyze.py assigned to a Galápagos island
  filter(best != "-", !is.na(best)) %>%
  
  # Step 2: drop records whose coordinates place them clearly on the mainland.
  # Records with no coordinates (NA) are kept — resolved by locality text alone.
  filter(is.na(decimalLongitude_std) | decimalLongitude_std <= MAINLAND_LON_CUTOFF) %>%
  
  # Step 3: remove contamination from mainland provinces whose locality text
  # does not mention Galápagos.
  #
  # Why three conditions?
  #   - stateProvince = Galápagos (any spelling)  → clearly valid, keep.
  #   - stateProvince = NA                        → many old records lack province;
  #                                                 keep rather than discard wholesale.
  #   - locality text mentions "galápago"         → keeps legitimate records where
  #                                                 stateProvince reflects the holding
  #                                                 institution rather than collection
#                                                 site (e.g. Smithsonian/Colón, Panama).
#   Records that fail all three are name-only matches where a mainland place
#   name (e.g. "Morona Santiago") matched an island name — discard these.
mutate(
  .province_galapagos = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
  .province_unknown   = is.na(stateProvince),
  .latlon_confirmed   = !is.na(latlon) & latlon != "-",
  .locality_galapagos = str_detect(
    paste(
      coalesce(locality,         ""),
      coalesce(verbatimLocality, ""),
      coalesce(island,           ""),
      coalesce(islandGroup,      ""),
      sep = " "
    ),
    GALAPAGOS_PATTERN
  )
) %>%
  filter(.province_galapagos | .province_unknown | .latlon_confirmed | .locality_galapagos) %>%
  select(-starts_with(".")) %>% 
  
  filter(basisOfRecord %in% c(
    "PRESERVED_SPECIMEN",
    "FOSSIL_SPECIMEN",   "MATERIAL_SAMPLE",
    "LIVING_SPECIMEN",   "OCCURRENCE"
  )) %>% 
  
  filter(class %in% c("Aves","Mammalia","Squamata","Testudines"))
  

n_after_step1 <- n_resolved
n_after_step2 <- sum(
  ecuador_data_merged$best != "-" & !is.na(ecuador_data_merged$best) &
    (is.na(suppressWarnings(as.numeric(ecuador_data_merged$decimalLongitude_std))) |
       suppressWarnings(as.numeric(ecuador_data_merged$decimalLongitude_std)) <= MAINLAND_LON_CUTOFF)
)

cat(sprintf(
  "Filter summary:\n  Resolved to island by analyze.py : %d\n  After mainland coordinate filter  : %d  (-%d)\n  After province/locality filter    : %d  (-%d)\n",
  n_after_step1,
  n_after_step2, n_after_step1 - n_after_step2,
  nrow(galapagos_specimens), n_after_step2 - nrow(galapagos_specimens)
))

cat("\nstateProvince breakdown of retained records:\n")
galapagos_specimens %>%
  mutate(province_group = case_when(
    str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN) ~ "Galápagos province",
    is.na(stateProvince)                                        ~ "Province unknown (NA)",
    TRUE                                                        ~ "Other province (locality confirmed)"
  )) %>%
  count(province_group) %>%
  print()

