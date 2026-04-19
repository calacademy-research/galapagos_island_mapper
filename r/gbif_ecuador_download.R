# =========================================================
# Galápagos Specimens: GBIF Download and Island Assignment
# =========================================================
# Downloads all Ecuador preserved/fossil/material specimens
# from GBIF (single current pull), merges island assignments
# from analyze.py, and filters to confirmed Galápagos records.
#
# This is ONE of two parallel pipelines:
#   • This script  — single current GBIF Ecuador download
#   • gbif_data_ingester.R — concatenates multiple GBIF pulls
#     across time (captures records that may have been removed)
#
# Workflow for this script:
#   1. (Once) Submit GBIF download job, wait for email
#   2. Retrieve + import the DwC-A archive (REDOWNLOAD=TRUE)
#   3. bash analyze.sh <ecuador_occurrences.tsv>
#      → results.tsv
#   4. Re-run this script (REDOWNLOAD=FALSE) to merge
#      analyze.py results and write galapagos_specimens.tsv
# =========================================================

library(dplyr)
library(stringr)
library(readr)
library(tidyr)
library(rgbif)
library(data.table)

# =========================================================
# CONFIG
# =========================================================

DOWNLOAD_KEY      <- "0025802-260409193756587"
RAW_DATA_DIR      <- "~/Dropbox/Galapagos_data/raw_data_from_gbif/"
INPUT_TSV         <- "~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv"
RESULTS_TSV       <- "~/galapagos_island_mapper/results.tsv"
OUTPUT_DIR        <- "~/Dropbox/Galapagos_data/output/"

# Galápagos easternmost point is ~-89.2°; anything east of
# -88° is definitively mainland Ecuador.
MAINLAND_LON_CUTOFF <- -88

# Set TRUE only when you need a fresh GBIF pull.
# Set FALSE to skip downloading and load from INPUT_TSV.
REDOWNLOAD <- FALSE

# =========================================================
# SECTION 1: HELPER FUNCTIONS
# =========================================================

# Convert blank strings / "NA" / "NULL" to NA and trim whitespace
clean_characters <- function(df) {
  df %>%
    mutate(across(
      where(is.character),
      ~ .x %>%
        str_trim() %>%
        na_if("") %>%
        na_if("NA") %>%
        na_if("NULL")
    ))
}

# Standardize a text field: lowercase, trim, collapse internal whitespace
std_text <- function(x) {
  x %>%
    str_trim() %>%
    str_squish() %>%
    str_to_lower()
}

# Safe column getter: returns NA vector if column is absent from df
get_col <- function(df, col) {
  if (col %in% names(df)) df[[col]] else rep(NA_character_, nrow(df))
}

# Count non-missing fields per row (higher score = richer record)
score_record_richness <- function(df) {
  fields_to_score <- c(
    "occurrenceID", "catalogNumber", "institutionCode", "collectionCode",
    "datasetKey", "scientificName", "species", "acceptedScientificName",
    "decimalLatitude", "decimalLongitude", "countryCode", "stateProvince",
    "island", "locality", "eventDate", "year", "month", "day",
    "recordedBy", "identifiedBy", "basisOfRecord",
    "stateProvince",	"county",	"municipality",	"verbatimLocality",	"locationRemarks"
  )
  present_fields <- intersect(fields_to_score, names(df))
  if (length(present_fields) == 0) return(rep(0L, nrow(df)))
  df %>%
    transmute(across(all_of(present_fields), ~ !is.na(.x) & .x != "")) %>%
    mutate(richness_score = rowSums(across(everything()))) %>%
    pull(richness_score)
}

# =========================================================
# SECTION 2: GBIF DOWNLOAD  (skipped unless REDOWNLOAD=TRUE)
# =========================================================

if (REDOWNLOAD) {

  # Submit download request (GBIF emails when ready — may take minutes to hours)
  # occ_download(
  #   pred("country", "EC"),
  #   pred_or(
  #     pred("basisOfRecord", "PRESERVED_SPECIMEN"),
  #     pred("basisOfRecord", "FOSSIL_SPECIMEN"),
  #     pred("basisOfRecord", "MATERIAL_SAMPLE")
  #   ),
  #   pred("occurrenceStatus", "PRESENT"),
  #   format = "DWCA"
  # )
  # Update DOWNLOAD_KEY above with the key from occ_download_queue()

  # Retrieve and import the completed download
  ecuador_dwca <- occ_download_get(DOWNLOAD_KEY, path = RAW_DATA_DIR, overwrite = TRUE)
  ecuador_data <- occ_download_import(ecuador_dwca)

  # Save provenance metadata
  download_info <- tibble(
    key  = DOWNLOAD_KEY,
    doi  = occ_download_meta(DOWNLOAD_KEY)$doi,
    date = Sys.Date()
  )
  write_csv(download_info, file.path(OUTPUT_DIR, "download_metadata.csv"))

  # Export full dataset as TSV for analyze.py
  write.table(
    ecuador_data,
    INPUT_TSV,
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE,
    na        = ""
  )
  cat("Exported", nrow(ecuador_data), "rows to", INPUT_TSV, "\n")
  cat("Next step: bash analyze.sh", INPUT_TSV, "\n")

} else {

  # Load from the previously exported TSV (after analyze.py has been run)
  ecuador_data <- read_tsv(
    INPUT_TSV,
    col_types    = cols(.default = col_character()),
    show_col_types = FALSE
  )
  cat("Loaded", nrow(ecuador_data), "rows from", INPUT_TSV, "\n")

}

# =========================================================
# SECTION 3: STANDARDIZE KEY FIELDS
# =========================================================

ecuador_data_std <- ecuador_data %>%
  mutate(
    occurrenceID_std           = std_text(get_col(., "occurrenceID")),
    catalogNumber_std          = std_text(get_col(., "catalogNumber")),
    institutionCode_std        = std_text(get_col(., "institutionCode")),
    collectionCode_std         = std_text(get_col(., "collectionCode")),
    publisher_std              = std_text(get_col(., "publisher")),
    datasetKey_std             = std_text(get_col(., "datasetKey")),
    scientificName_std         = std_text(get_col(., "scientificName")),
    species_std                = std_text(get_col(., "species")),
    acceptedScientificName_std = std_text(get_col(., "acceptedScientificName")),
    recordedBy_std             = std_text(get_col(., "recordedBy")),
    eventDate_std              = std_text(get_col(., "eventDate")),
    locality_std               = std_text(get_col(., "locality")),
    island_std                 = std_text(get_col(., "island")),
    decimalLatitude_std        = suppressWarnings(as.numeric(get_col(., "decimalLatitude"))),
    decimalLongitude_std       = suppressWarnings(as.numeric(get_col(., "decimalLongitude")))
  )

# =========================================================
# SECTION 4: MERGE WITH analyze.py ISLAND ASSIGNMENTS
# =========================================================

results <- fread(RESULTS_TSV) %>%
  clean_characters() %>%
  mutate(across(everything(), as.character)) %>%
  select(gbifID, best, name, latlon)

ecuador_data_std <- ecuador_data_std %>%
  clean_characters() %>%
  mutate(across(everything(), as.character))

ecuador_data_merged <- inner_join(ecuador_data_std, results, by = "gbifID")

# Sanity check: records in the data file that have no entry in results.tsv
missing_from_results <- anti_join(ecuador_data_std, results, by = "gbifID")
if (nrow(missing_from_results) > 0) {
  warning(sprintf(
    "%d records in ecuador_data_std have no match in results.tsv — were they processed by analyze.py?",
    nrow(missing_from_results)
  ))
}

# =========================================================
# SECTION 5: FILTER TO GALÁPAGOS SPECIMENS
# =========================================================

# Matches all common spellings: Galapagos, Galápagos, Galpagos, etc.
GALAPAGOS_PATTERN <- regex("al[aá]?pag", ignore_case = TRUE)

n_resolved <- sum(ecuador_data_merged$best != "-" & !is.na(ecuador_data_merged$best))

# NOTE: Section 4 converts all columns to character for join compatibility.
# decimalLongitude_std is therefore character here; we must convert back to
# numeric explicitly before any comparison, otherwise R uses lexicographic
# ordering ("-91.5" > "-88" lexicographically even though -91.5 < -88
# numerically), which silently drops Galápagos coordinates and keeps mainland
# ones — exactly backwards.
galapagos_specimens <- ecuador_data_merged %>%
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
  #                          No further province check needed.
  #
  #  B. province_galapagos → stateProvince explicitly says "Galápagos"
  #                          (any spelling/accent variant).  Clearly valid.
  #
  #  C. province_unknown   → stateProvince is NA.  Many old museum records
  #                          lack this field; keeping them risks minor
  #                          false-positives but discarding wholesale would
  #                          lose large legitimate collections.
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
n_after_step1 <- n_resolved
n_after_step2 <- ecuador_data_merged %>%
  filter(best != "-", !is.na(best)) %>%
  mutate(lon_num = suppressWarnings(as.numeric(decimalLongitude_std))) %>%
  filter(is.na(lon_num) | lon_num <= MAINLAND_LON_CUTOFF) %>%
  nrow()

cat(sprintf(
  "Filter summary:\n  Resolved to island by analyze.py : %d\n  After mainland coordinate filter  : %d  (-%d)\n  After province/locality filter    : %d  (-%d)\n",
  n_after_step1,
  n_after_step2, n_after_step1 - n_after_step2,
  nrow(galapagos_specimens), n_after_step2 - nrow(galapagos_specimens)
))

# ── Which condition kept each record? ────────────────────
step3_pool <- ecuador_data_merged %>%
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
# SECTION 6: WRITE OUTPUTS
# =========================================================

# ── 6a. Main output: confirmed Galápagos specimens ────────
out_file <- file.path(OUTPUT_DIR, "galapagos_specimens.tsv")
write_tsv(galapagos_specimens, out_file)
cat("Written:", out_file, "\n")

# ── 6b. Unresolved Galápagos records ─────────────────────
# These records have stateProvince = Galápagos (clearly from
# the archipelago) but analyze.py could not assign them to a
# specific island — typically because they lack specific
# locality text and have no usable coordinates.
#
# Saved with ALL columns intact so we can look for locality
# data in less-standard fields (habitat, verbatimLocality,
# georeferenceRemarks, locationRemarks, occurrenceRemarks, etc.)
galapagos_unresolved <- ecuador_data_merged %>%
  filter(best == "-" | is.na(best)) %>%
  filter(str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN))

cat(sprintf(
  "\nUnresolved Galápagos records: %d total\n",
  nrow(galapagos_unresolved)
))

# Show class breakdown so we know which groups are affected
cat("  By class:\n")
galapagos_unresolved %>%
  mutate(class_label = coalesce(class, "(no class)")) %>%
  count(class_label, sort = TRUE) %>%
  slice_head(n = 15) %>%
  print(n = 15)

# Show which columns have any data — helps spot non-standard
# locality fields that might allow island assignment
cat("\n  Columns with at least some non-NA values (excluding always-populated fields):\n")
always_populated <- c("gbifID", "occurrenceID", "basisOfRecord", "species",
                      "scientificName", "class", "stateProvince", "countryCode")
galapagos_unresolved %>%
  select(-any_of(always_populated)) %>%
  summarise(across(everything(), ~ sum(!is.na(.x) & .x != "" & .x != "-"))) %>%
  pivot_longer(everything(), names_to = "column", values_to = "n_non_empty") %>%
  filter(n_non_empty > 0) %>%
  arrange(desc(n_non_empty)) %>%
  print(n = 40)

unresolved_file <- file.path(OUTPUT_DIR, "galapagos_unresolved.tsv")
write_tsv(galapagos_unresolved, unresolved_file)
cat("\nWritten:", unresolved_file, "\n")

