# =========================================================
# Galápagos: GBIF Download Filtered by GADM = ECU.9_1
# =========================================================
# Downloads all GBIF occurrence records for the Galápagos
# Islands by querying directly on GADM administrative area
# ECU.9_1 (the Galápagos province polygon in gadm.org).
#
# Because GBIF applies the GADM filter server-side, every
# record in the download is already known to fall within the
# Galápagos province boundary — no post-hoc contamination
# filtering is needed.  The resulting data frame can then be
# subsetted freely (by basisOfRecord, class, island, etc.).
#
# Comparison purpose: use this as a "ground truth" download
# to evaluate how many records the Ecuador-based pipelines
# (gbif_ecuador_download.R, gbif_data_ingester.R) recover
# or miss.
#
# Workflow:
#   1. Set REDOWNLOAD = TRUE, run Section 2 to submit job.
#      GBIF emails when the download is ready (minutes–hours).
#      Update DOWNLOAD_KEY with the key from the email / from
#      occ_download_queue().
#   2. Run Section 2 again (or re-source) to retrieve + import.
#      Set REDOWNLOAD = FALSE after first successful retrieval.
#   3. bash analyze.sh <path to galapagos_gadm_occurrences.tsv>
#      → results.tsv
#   4. Run Section 3 to merge island assignments back in.
#   5. Run Section 4 to filter / subset as needed.
# =========================================================

library(dplyr)
library(stringr)
library(readr)
library(rgbif)
library(data.table)

# =========================================================
# CONFIG
# =========================================================

# Paste the key returned by occ_download() / from the GBIF
# email here before running Section 2 in retrieval mode.
DOWNLOAD_KEY  <- ""

RAW_DATA_DIR  <- "~/Dropbox/Galapagos_data/raw_data_from_gbif/"
INPUT_TSV     <- "~/Dropbox/Galapagos_data/input/galapagos_gadm_occurrences.tsv"
RESULTS_TSV   <- "~/galapagos_island_mapper/results.tsv"
OUTPUT_DIR    <- "~/Dropbox/Galapagos_data/output/"

# Set TRUE to submit a new download job or retrieve a completed one.
# Set FALSE once the TSV is on disk and you just want to work with it.
REDOWNLOAD <- FALSE

# =========================================================
# SECTION 1: HELPER FUNCTIONS
# =========================================================

clean_characters <- function(df) {
  df %>%
    mutate(across(
      where(is.character),
      ~ .x %>% str_trim() %>% na_if("") %>% na_if("NA") %>% na_if("NULL")
    ))
}

# =========================================================
# SECTION 2: GBIF DOWNLOAD  (skipped unless REDOWNLOAD=TRUE)
# =========================================================

if (REDOWNLOAD) {

  if (nchar(DOWNLOAD_KEY) == 0) {
    # ── Submit a new download request ──────────────────────
    # GBIF will email you when the job is ready.
    # Uncomment and run once; then paste the returned key into
    # DOWNLOAD_KEY above and re-run with REDOWNLOAD = TRUE.
    #
    # dl <- occ_download(
    #   pred("gadmGid", "ECU.9_1"),
    #   pred("occurrenceStatus", "PRESENT"),
    #   format = "DWCA"
    # )
    # occ_download_queue()   # check status
    stop("Set DOWNLOAD_KEY to the key from occ_download() / GBIF email, then re-run.")
  }

  # ── Retrieve and import a completed download ────────────
  cat("Retrieving download", DOWNLOAD_KEY, "...\n")
  gadm_dwca  <- occ_download_get(DOWNLOAD_KEY,
                                  path     = RAW_DATA_DIR,
                                  overwrite = TRUE)
  galapagos_gadm_raw <- occ_download_import(gadm_dwca)

  cat(sprintf("Downloaded: %d rows, %d columns\n",
              nrow(galapagos_gadm_raw), ncol(galapagos_gadm_raw)))

  # Save as TSV for analyze.py.
  # write.table (not write_tsv) so no quoting — required for Python
  # QUOTE_NONE mode.  clean_delimiters() removes embedded tabs/newlines.
  galapagos_gadm_raw %>%
    mutate(across(where(is.character),
                  ~ str_replace_all(.x, "[\t\n\r]", " "))) %>%
    write.table(
      INPUT_TSV,
      sep       = "\t",
      row.names = FALSE,
      quote     = FALSE,
      na        = ""
    )
  cat("Saved to:", INPUT_TSV, "\n")
  cat("Next step: bash analyze.sh", INPUT_TSV, "\n\n")

} else {

  # ── Load previously saved TSV ───────────────────────────
  galapagos_gadm_raw <- read_tsv(
    INPUT_TSV,
    col_types      = cols(.default = col_character()),
    show_col_types = FALSE
  ) %>%
    clean_characters()

  cat(sprintf("Loaded: %d rows, %d columns\n",
              nrow(galapagos_gadm_raw), ncol(galapagos_gadm_raw)))
}

# Quick sanity check: all records should be ECU.9_1
cat("\nlevel1Gid values (should be only ECU.9_1):\n")
galapagos_gadm_raw %>%
  count(level1Gid, sort = TRUE) %>%
  slice_head(n = 5) %>%
  print()

cat("\nbasisOfRecord breakdown:\n")
galapagos_gadm_raw %>% count(basisOfRecord, sort = TRUE) %>% print()

cat("\nTop 10 classes:\n")
galapagos_gadm_raw %>% count(class, sort = TRUE) %>% slice_head(n = 10) %>% print()

# =========================================================
# SECTION 3: MERGE ISLAND ASSIGNMENTS FROM analyze.py
# =========================================================
# Run analyze.sh on INPUT_TSV first (see Section 2 output),
# then come back here.

results <- fread(RESULTS_TSV) %>%
  mutate(gbifID = as.character(gbifID)) %>%
  select(gbifID, best, name, latlon)

galapagos_gadm_data <- galapagos_gadm_raw %>%
  mutate(gbifID = as.character(gbifID)) %>%
  inner_join(results, by = "gbifID")

missing <- nrow(galapagos_gadm_raw) - nrow(galapagos_gadm_data)
if (missing > 0)
  warning(sprintf("%d records have no match in results.tsv — re-run analyze.py?",
                  missing))

cat(sprintf("\nAfter merging analyze.py results: %d records\n", nrow(galapagos_gadm_data)))
cat(sprintf("  Resolved to an island : %d  (%.1f%%)\n",
            sum(galapagos_gadm_data$best != "-", na.rm = TRUE),
            100 * mean(galapagos_gadm_data$best != "-", na.rm = TRUE)))

# =========================================================
# SECTION 4: FILTER TO SUBSETS OF INTEREST
# =========================================================
# The full data frame (galapagos_gadm_data) contains all GBIF
# record types for Galápagos.  Subset as needed below.

# ── 4a. All records assigned to a specific island ─────────
galapagos_gadm_resolved <- galapagos_gadm_data %>%
  filter(best != "-", !is.na(best))

cat(sprintf("\nResolved records: %d\n", nrow(galapagos_gadm_resolved)))

# ── 4b. Museum / physical specimens only ──────────────────
galapagos_gadm_specimens <- galapagos_gadm_resolved %>%
  filter(basisOfRecord %in% c(
    "PRESERVED_SPECIMEN", "FOSSIL_SPECIMEN",
    "MATERIAL_SAMPLE",    "LIVING_SPECIMEN"
  ))

cat(sprintf("  of which specimens (preserved/fossil/material/living): %d\n",
            nrow(galapagos_gadm_specimens)))

# ── Island breakdown ───────────────────────────────────────
cat("\nIsland breakdown (all resolved records):\n")
galapagos_gadm_resolved %>%
  count(best, sort = TRUE) %>%
  print()

cat("\nIsland breakdown (specimens only):\n")
galapagos_gadm_specimens %>%
  count(best, sort = TRUE) %>%
  print()

# ── Top institutions (specimens) ───────────────────────────
cat("\nTop institutions (specimens):\n")
galapagos_gadm_specimens %>%
  count(institutionCode, sort = TRUE) %>%
  slice_head(n = 15) %>%
  print()

# =========================================================
# SECTION 5: WRITE OUTPUTS
# =========================================================

out_all  <- file.path(OUTPUT_DIR, "galapagos_gadm_all.tsv")
out_spec <- file.path(OUTPUT_DIR, "galapagos_gadm_specimens.tsv")

write_tsv(galapagos_gadm_data,      out_all)
write_tsv(galapagos_gadm_specimens, out_spec)

cat("\nWritten:\n")
cat(" ", out_all,  "\n")
cat(" ", out_spec, "\n")
