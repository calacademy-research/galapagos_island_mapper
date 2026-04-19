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
  ~source_file,                                                                            ~pull_date,    ~download_doi,
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_gbif_12APR2026.txt",         "2026-04-14",  "https://doi.org/10.15468/dl.4r5r7j",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_gbif_24SEP2025.txt",         "2025-09-24",  "https://doi.org/10.15468/dl.hzhmgr",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_28JAN2024.txt",              "2024-01-28",  "https://doi.org/10.15468/dl.3zpgd8",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_15APR26.txt",                "2026-04-15",  "10.15468/dl.9xv8jq",
  "/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_0025802-260409193756587.txt","2026-04-17",  "10.15468/dl.3jeq3p",
) %>%
  mutate(pull_date = as.Date(pull_date))

OUTPUT_DIR  <- "~/Dropbox/Galapagos_data/output/"
RESULTS_TSV <- "~/galapagos_island_mapper/results.tsv"

# Galápagos easternmost point is ~-89.2°; anything east of
# -88° is definitively mainland Ecuador.
MAINLAND_LON_CUTOFF <- -88

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

# Load results produced by analyze.py.
# fread reads gbifID as integer64; convert to character so the join
# key type matches gbif_current_best (which has character gbifID from
# the TSV files read by readr::read_tsv).
results <- fread(RESULTS_TSV) %>%
  clean_characters() %>%
  mutate(gbifID = as.character(gbifID)) %>%
  select(gbifID, best, name, latlon)

# Ensure gbifID in the deduplicated records is also character
gbif_current_best <- gbif_current_best %>%
  mutate(gbifID = as.character(gbifID))

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
# SECTION 11: FILTER TO GALÁPAGOS RECORDS
# =========================================================
# Mirrors the filter logic in gbif_ecuador_download.R Section 5.
# decimalLongitude_std was created as numeric in Section 4 and
# is NOT converted to character here, so numeric comparison works
# directly.  We still extract lon_num explicitly for clarity and
# safety (guards against any future upstream change).

# Matches all common spellings: Galapagos, Galápagos, Galpagos, etc.
GALAPAGOS_PATTERN <- regex("al[aá]?pag", ignore_case = TRUE)

n_resolved <- sum(galapagos_data$best != "-" & !is.na(galapagos_data$best))

galapagos_specimens <- galapagos_data %>%
  mutate(lon_num = suppressWarnings(as.numeric(decimalLongitude_std))) %>%

  # Step 1: keep only records that analyze.py assigned to a Galápagos island
  filter(best != "-", !is.na(best)) %>%

  # Step 2: drop records whose coordinates place them clearly on the mainland.
  # Records with no coordinates (NA lon_num) are kept — resolved by name/locality.
  filter(is.na(lon_num) | lon_num <= MAINLAND_LON_CUTOFF) %>%

  # Step 3: remove remaining contamination — records whose island assignment
  # almost certainly came from a mainland place name rather than a genuine
  # Galápagos locality description.
  #
  # Four conditions — a record is kept if ANY is TRUE:
  #
  #  A. latlon_confirmed   → lat/lon resolver placed it on a Galápagos island.
  #                          GPS-derived coordinates are highly reliable.
  #
  #  B. province_galapagos → stateProvince explicitly says "Galápagos"
  #                          (any spelling/accent variant).  Clearly valid.
  #
  #  C. province_unknown   → stateProvince is NA.  Many old museum records
  #                          lack this field; keeping them avoids wholesale
  #                          loss of large legitimate collections.
  #
  #  D. locality_galapagos → locality / verbatimLocality / island /
  #                          islandGroup text contains "galápago" (any
  #                          spelling).  Clearly Galápagos-labelled.
  #
  #  Records failing all four are name-only matches where a mainland place
  #  name (e.g. "Morona Santiago" → santiago, "Santa Elena" → santa fe)
  #  happened to match an island name — these are discarded.
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
  filter(
    .latlon_confirmed   |   # (A) lat/lon resolver → always trust
    .province_galapagos |   # (B) stateProvince = Galápagos
    .province_unknown   |   # (C) no province info (old records)
    .locality_galapagos     # (D) locality text mentions Galápagos
  ) %>%
  select(-starts_with("."), -lon_num)

# ── Filter summary ────────────────────────────────────────
n_after_step2 <- galapagos_data %>%
  filter(best != "-", !is.na(best)) %>%
  mutate(lon_num = suppressWarnings(as.numeric(decimalLongitude_std))) %>%
  filter(is.na(lon_num) | lon_num <= MAINLAND_LON_CUTOFF) %>%
  nrow()

cat(sprintf(
  "Filter summary:\n  Resolved to island by analyze.py : %d\n  After mainland coordinate filter  : %d  (-%d)\n  After province/locality filter    : %d  (-%d)\n",
  n_resolved,
  n_after_step2, n_resolved - n_after_step2,
  nrow(galapagos_specimens), n_after_step2 - nrow(galapagos_specimens)
))

# ── Which condition kept each record? ────────────────────
step3_pool <- galapagos_data %>%
  mutate(lon_num = suppressWarnings(as.numeric(decimalLongitude_std))) %>%
  filter(best != "-", !is.na(best)) %>%
  filter(is.na(lon_num) | lon_num <= MAINLAND_LON_CUTOFF) %>%
  mutate(
    .province_galapagos = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
    .province_unknown   = is.na(stateProvince),
    .latlon_confirmed   = !is.na(latlon) & latlon != "-",
    .locality_galapagos = str_detect(
      paste(coalesce(locality,""), coalesce(verbatimLocality,""),
            coalesce(island,""),   coalesce(islandGroup,""), sep=" "),
      GALAPAGOS_PATTERN),
    .kept = .latlon_confirmed | .province_galapagos | .province_unknown |
            .locality_galapagos
  )

cat("\nRecords kept by each filter condition (not mutually exclusive):\n")
tribble(
  ~condition,                        ~n,
  "(A) lat/lon resolved",            sum(step3_pool$.latlon_confirmed,   na.rm=TRUE),
  "(B) province = Galápagos",        sum(step3_pool$.province_galapagos, na.rm=TRUE),
  "(C) province = NA",               sum(step3_pool$.province_unknown,   na.rm=TRUE),
  "(D) locality mentions Galápagos", sum(step3_pool$.locality_galapagos, na.rm=TRUE),
  "TOTAL kept",                      sum( step3_pool$.kept,              na.rm=TRUE),
  "TOTAL dropped (contamination)",   sum(!step3_pool$.kept,              na.rm=TRUE)
) %>% print()

cat("\nstateProvince breakdown of retained records:\n")
galapagos_specimens %>%
  mutate(province_group = case_when(
    str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN) ~ "Galápagos province",
    is.na(stateProvince)                                        ~ "Province unknown (NA)",
    TRUE                                                        ~ "Other province (latlon confirmed)"
  )) %>%
  count(province_group, sort = TRUE) %>%
  print()

cat("\nCAS Aves surviving filter (spot-check for recovery):\n")
galapagos_specimens %>%
  filter(coalesce(institutionCode, "") == "CAS",
         coalesce(class, "")           == "Aves") %>%
  summarise(n = n(), islands = n_distinct(best)) %>%
  print()

cat("\nTop stateProvince values among dropped records (contamination check):\n")
step3_pool %>%
  filter(!.kept) %>%
  count(stateProvince, sort = TRUE) %>%
  slice_head(n = 15) %>%
  print()

# =========================================================
# SECTION 12: WRITE OUTPUTS
# =========================================================
# Output files use the _multipull suffix to distinguish them from
# the equivalent files produced by gbif_ecuador_download.R, since
# both pipelines write to the same OUTPUT_DIR.

# ── 12a. Main output: confirmed Galápagos specimens ───────
out_file <- file.path(OUTPUT_DIR, "galapagos_specimens_multipull.tsv")
write_tsv(galapagos_specimens, out_file)
cat("Written:", out_file, "\n")

# ── 12b. Unresolved Galápagos records ────────────────────
# Records with stateProvince = Galápagos that analyze.py could not
# assign to a specific island — typically old records without
# specific locality text or usable coordinates.
galapagos_unresolved <- galapagos_data %>%
  filter(best == "-" | is.na(best)) %>%
  filter(str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN))

cat(sprintf(
  "\nUnresolved Galápagos records: %d total\n",
  nrow(galapagos_unresolved)
))

cat("  By class:\n")
galapagos_unresolved %>%
  mutate(class_label = coalesce(class, "(no class)")) %>%
  count(class_label, sort = TRUE) %>%
  slice_head(n = 15) %>%
  print(n = 15)

unresolved_file <- file.path(OUTPUT_DIR, "galapagos_unresolved_multipull.tsv")
write_tsv(galapagos_unresolved, unresolved_file)
cat("\nWritten:", unresolved_file, "\n")
