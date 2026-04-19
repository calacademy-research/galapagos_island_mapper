# =========================================================
# Diagnostic: Tracing CAS Bird Loss Through Both Pipelines
# =========================================================
# There are two independent pipelines:
#
#   Pipeline A  gbif_ecuador_download.R
#               Input:  ecuador_occurrences.tsv  (single current pull)
#               Output: galapagos_specimens.tsv
#
#   Pipeline B  gbif_data_ingester.R
#               Input:  multiple GBIF pull files concatenated
#               Output: gbif_occurrences_deduplicated_best.tsv
#               Then:   analyze.py → results.tsv
#               Then:   Section 10 → galapagos_data in R
#
# This script examines BOTH inputs and traces, step by step,
# where the ~9,931 CAS Aves records from 2025 are being lost.
#
# Run AFTER analyze.py has been run on whichever pipeline
# you want to diagnose (results.tsv must exist).
# =========================================================

library(dplyr)
library(readr)
library(stringr)
library(data.table)

# ── CONFIG ────────────────────────────────────────────────
# Pipeline A
ECUADOR_TSV         <- "~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv"
# Pipeline B
DEDUP_BEST_TSV      <- "~/Dropbox/Galapagos_data/output/gbif_occurrences_deduplicated_best.tsv"
# Shared (from whichever pipeline was run most recently)
RESULTS_TSV         <- "~/galapagos_island_mapper/results.tsv"
# Reference: the 9,931 CAS birds from 2025
CAS_2025_TSV        <- "~/Dropbox/Galapagos_data/output/cas_birds_2025.tsv"

GALAPAGOS_PATTERN   <- regex("al[aá]?pag", ignore_case = TRUE)
MAINLAND_LON_CUTOFF <- -88

# ── HELPERS ───────────────────────────────────────────────
sep  <- function(lbl = "") cat("\n", strrep("=", 60), "\n", lbl, "\n", sep = "")
pct  <- function(n, tot) sprintf("%d  (%.1f%%)", n, 100 * n / max(tot, 1))

read_cols <- function(path, cols) {
  all_cols <- names(fread(path.expand(path), nrows = 0))
  keep     <- intersect(cols, all_cols)
  fread(path.expand(path), sep = "\t", quote = "", na.strings = "",
        select = keep, data.table = FALSE) %>%
    mutate(gbifID = as.character(gbifID)) %>%
    mutate(across(where(is.character), ~ na_if(str_trim(.), "")))
}

key_cols <- c("gbifID", "institutionCode", "class", "basisOfRecord",
              "stateProvince", "countryCode",
              "decimalLatitude", "decimalLongitude",
              "locality", "verbatimLocality", "island", "islandGroup",
              "county", "species", "scientificName")

# =========================================================
# SECTION 0: Load reference CAS 2025 dataset
# =========================================================
sep("SECTION 0: REFERENCE DATASET (cas_birds_2025.tsv)")

cas_ref <- fread(path.expand(CAS_2025_TSV), sep = "\t", quote = "",
                 na.strings = "", data.table = FALSE) %>%
  mutate(gbifID = as.character(gbifID)) %>%
  mutate(across(where(is.character), ~ na_if(str_trim(.), ""))) %>%
  filter(!is.na(gbifID))

cat(sprintf("Reference CAS 2025 birds : %d records\n", nrow(cas_ref)))
cat(sprintf("Distinct gbifIDs         : %d\n", n_distinct(cas_ref$gbifID)))
ref_ids <- cas_ref$gbifID

# =========================================================
# SECTION 1: results.tsv — which pipeline does it match?
# =========================================================
sep("SECTION 1: results.tsv COVERAGE")

results <- fread(path.expand(RESULTS_TSV), sep = "\t", na.strings = "",
                 data.table = FALSE) %>%
  mutate(across(everything(), ~ na_if(as.character(.), ""))) %>%
  mutate(gbifID = as.character(gbifID))

cat(sprintf("results.tsv rows         : %d\n", nrow(results)))
cat(sprintf("2025 CAS IDs in results  : %d / %d\n",
            sum(ref_ids %in% results$gbifID), length(ref_ids)))
cat(sprintf("  of those, resolved     : %d\n",
            sum(results$gbifID %in% ref_ids & results$best != "-" & !is.na(results$best))))

# =========================================================
# SECTION 2: Trace Pipeline A (ecuador_occurrences.tsv)
# =========================================================
sep("SECTION 2: PIPELINE A — ecuador_occurrences.tsv")

if (!file.exists(path.expand(ECUADOR_TSV))) {
  cat("File not found:", ECUADOR_TSV, "\n")
} else {
  ecu <- read_cols(ECUADOR_TSV, key_cols)
  cat(sprintf("Total rows               : %d\n", nrow(ecu)))
  cat(sprintf("CAS rows                 : %d\n", sum(coalesce(ecu$institutionCode, "") == "CAS")))
  cat(sprintf("CAS Aves rows            : %d\n",
              sum(coalesce(ecu$institutionCode, "") == "CAS" & coalesce(ecu$class, "") == "Aves")))
  cat(sprintf("2025 CAS IDs present     : %d / %d\n",
              sum(ref_ids %in% ecu$gbifID), length(ref_ids)))

  cas_a <- ecu %>%
    filter(coalesce(institutionCode, "") == "CAS", coalesce(class, "") == "Aves")
  n0 <- nrow(cas_a)

  if (n0 > 0) {
    # Join with results
    cas_a_res <- cas_a %>% inner_join(results, by = "gbifID")
    n_in_res  <- nrow(cas_a_res)
    cat(sprintf("\nCAS Aves in results.tsv  : %s  (of %d in input file)\n",
                pct(n_in_res, n0), n0))
    cat(sprintf("  NOTE: results.tsv was built from whichever pipeline was last run.\n"))
    cat(sprintf("  If it was built from Pipeline B, many of these joins will fail.\n"))

    n_resolved <- sum(cas_a_res$best != "-" & !is.na(cas_a_res$best))
    cat(sprintf("  Of joined: resolved to island : %s\n", pct(n_resolved, n_in_res)))

    # Filter steps (mirrors gbif_ecuador_download.R Section 5)
    after_s1 <- cas_a_res %>% filter(best != "-", !is.na(best))
    after_s2 <- after_s1 %>%
      mutate(lon = suppressWarnings(as.numeric(decimalLongitude))) %>%
      filter(is.na(lon) | lon <= MAINLAND_LON_CUTOFF)
    after_s3 <- after_s2 %>%
      mutate(
        .prov_gal  = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
        .prov_na   = is.na(stateProvince),
        .ll_conf   = !is.na(latlon) & latlon != "-",
        .loc_gal   = str_detect(
          paste(coalesce(locality,""), coalesce(verbatimLocality,""),
                coalesce(island,""),   coalesce(islandGroup,""), sep=" "),
          GALAPAGOS_PATTERN)
      ) %>%
      filter(.prov_gal | .prov_na | .ll_conf | .loc_gal)

    cat(sprintf("\nFilter funnel (Pipeline A CAS Aves):\n"))
    cat(sprintf("  Start (CAS Aves in input)     : %d\n", n0))
    cat(sprintf("  → joined with results.tsv     : %d  (-%d gbifID mismatch)\n",
                n_in_res, n0 - n_in_res))
    cat(sprintf("  → after best != '-'           : %d  (-%d unresolved)\n",
                nrow(after_s1), n_in_res - nrow(after_s1)))
    cat(sprintf("  → after lon <= %d             : %d  (-%d mainland lon)\n",
                MAINLAND_LON_CUTOFF, nrow(after_s2), nrow(after_s1) - nrow(after_s2)))
    cat(sprintf("  → after Galápagos filter      : %d  (-%d province/locality)\n",
                nrow(after_s3), nrow(after_s2) - nrow(after_s3)))
    cat(sprintf("  Expected from 2025 ref        : 9931\n"))
    cat(sprintf("  Shortfall                     : %d\n", 9931 - nrow(after_s3)))
  }
}

# =========================================================
# SECTION 3: Trace Pipeline B (gbif_data_ingester.R output)
# =========================================================
sep("SECTION 3: PIPELINE B — gbif_occurrences_deduplicated_best.tsv")

if (!file.exists(path.expand(DEDUP_BEST_TSV))) {
  cat("File not found:", DEDUP_BEST_TSV, "\n")
} else {
  dd <- read_cols(DEDUP_BEST_TSV, key_cols)
  cat(sprintf("Total rows               : %d\n", nrow(dd)))
  cat(sprintf("CAS rows                 : %d\n", sum(coalesce(dd$institutionCode, "") == "CAS")))
  cat(sprintf("CAS Aves rows            : %d\n",
              sum(coalesce(dd$institutionCode, "") == "CAS" & coalesce(dd$class, "") == "Aves")))
  cat(sprintf("2025 CAS IDs present     : %d / %d\n",
              sum(ref_ids %in% dd$gbifID), length(ref_ids)))

  cas_b <- dd %>%
    filter(coalesce(institutionCode, "") == "CAS", coalesce(class, "") == "Aves")
  n0 <- nrow(cas_b)

  if (n0 > 0) {
    cas_b_res <- cas_b %>% inner_join(results, by = "gbifID")
    n_in_res  <- nrow(cas_b_res)
    cat(sprintf("\nCAS Aves in results.tsv  : %s  (of %d in input file)\n",
                pct(n_in_res, n0), n0))

    n_resolved <- sum(cas_b_res$best != "-" & !is.na(cas_b_res$best))
    cat(sprintf("  Of joined: resolved    : %s\n", pct(n_resolved, n_in_res)))

    # Pipeline B Section 10 only does inner_join + no Galápagos filter
    # (galapagos_data = all resolved records).  Show what would survive
    # IF we applied the same contamination filter as Pipeline A.
    after_s1 <- cas_b_res %>% filter(best != "-", !is.na(best))
    after_s2 <- after_s1 %>%
      mutate(lon = suppressWarnings(as.numeric(decimalLongitude))) %>%
      filter(is.na(lon) | lon <= MAINLAND_LON_CUTOFF)
    after_s3 <- after_s2 %>%
      mutate(
        .prov_gal  = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
        .prov_na   = is.na(stateProvince),
        .ll_conf   = !is.na(latlon) & latlon != "-",
        .loc_gal   = str_detect(
          paste(coalesce(locality,""), coalesce(verbatimLocality,""),
                coalesce(island,""),   coalesce(islandGroup,""), sep=" "),
          GALAPAGOS_PATTERN)
      ) %>%
      filter(.prov_gal | .prov_na | .ll_conf | .loc_gal)

    cat(sprintf("\nFilter funnel (Pipeline B CAS Aves):\n"))
    cat(sprintf("  Start (CAS Aves in dedup)     : %d\n", n0))
    cat(sprintf("  → joined with results.tsv     : %d  (-%d skipped by analyze.py)\n",
                n_in_res, n0 - n_in_res))
    cat(sprintf("  → after best != '-'           : %d  (-%d unresolved)\n",
                nrow(after_s1), n_in_res - nrow(after_s1)))
    cat(sprintf("  → after lon <= %d             : %d  (-%d mainland lon)\n",
                MAINLAND_LON_CUTOFF, nrow(after_s2), nrow(after_s1) - nrow(after_s2)))
    cat(sprintf("  → after Galápagos filter      : %d  (-%d province/locality)\n",
                nrow(after_s3), nrow(after_s2) - nrow(after_s3)))
    cat(sprintf("  Expected from 2025 ref        : 9931\n"))
    cat(sprintf("  Shortfall                     : %d\n", 9931 - nrow(after_s3)))

    # ── Where do the Galápagos-filtered ones fail? ──────────
    filtered_out <- after_s2 %>%
      mutate(
        .prov_gal = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
        .prov_na  = is.na(stateProvince),
        .ll_conf  = !is.na(latlon) & latlon != "-",
        .loc_gal  = str_detect(
          paste(coalesce(locality,""), coalesce(verbatimLocality,""),
                coalesce(island,""),   coalesce(islandGroup,""), sep=" "),
          GALAPAGOS_PATTERN)
      ) %>%
      filter(!(.prov_gal | .prov_na | .ll_conf | .loc_gal))

    if (nrow(filtered_out) > 0) {
      cat(sprintf("\n  Records removed by Galápagos filter (%d rows):\n", nrow(filtered_out)))
      cat("  stateProvince breakdown:\n")
      filtered_out %>%
        count(stateProvince, sort = TRUE) %>%
        slice_head(n = 15) %>%
        print()
      cat("  Sample of removed records:\n")
      filtered_out %>%
        select(gbifID, stateProvince, locality, island, latlon) %>%
        slice_head(n = 6) %>%
        print(width = 120)
    }
  }
}

# =========================================================
# SECTION 4: Is the shortfall a GBIF withdrawal?
# =========================================================
sep("SECTION 4: ARE 2025 CAS IDS MISSING FROM BOTH INPUTS?")

missing_from_ecu <- if (exists("ecu"))
  sum(!ref_ids %in% ecu$gbifID) else NA_integer_
missing_from_dd  <- if (exists("dd"))
  sum(!ref_ids %in% dd$gbifID)  else NA_integer_

cat(sprintf("2025 CAS IDs missing from ecuador_occurrences.tsv : %s\n",
            if (!is.na(missing_from_ecu)) as.character(missing_from_ecu) else "file not loaded"))
cat(sprintf("2025 CAS IDs missing from dedup_best.tsv          : %s\n",
            if (!is.na(missing_from_dd)) as.character(missing_from_dd) else "file not loaded"))

cat("\n")
cat("If both numbers are large, CAS has likely removed records from GBIF.\n")
cat("The multi-pull pipeline (Pipeline B) should recover more, because\n")
cat("gbif_data_ingester.R includes the 2025 pull where those records existed.\n")
cat("Check whether occurrence_gbif_24SEP2025.txt is in your pulls list.\n")

cat(strrep("=", 60), "\nDONE\n")
