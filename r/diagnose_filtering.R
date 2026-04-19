# =========================================================
# Diagnostic: Filter Quality and Record Completeness
# =========================================================
# Two problems to investigate:
#   1. Mainland contamination — inappropriate species slipping
#      through into galapagos_specimens
#   2. Missing records — vertebrate counts lower than expected
#
# Run this script and share the output so we can decide
# how to tighten/adjust the filtering pipeline.
# =========================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)

# =========================================================
# CONFIG
# =========================================================

SPECIMENS_FILE  <- "~/Dropbox/Galapagos_data/output/galapagos_specimens.tsv"
RESULTS_FILE    <- "~/galapagos_island_mapper/results.tsv"
FULL_INPUT_FILE <- "~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv"

TARGET_CLASSES  <- c("Aves", "Mammalia", "Testudines", "Squamata")

# =========================================================
# LOAD DATA
# =========================================================

cat("Loading galapagos_specimens...\n")
specimens <- read_tsv(SPECIMENS_FILE, col_types = cols(.default = col_character()),
                      show_col_types = FALSE)
cat(nrow(specimens), "records\n\n")

# =========================================================
# PART 1: MAINLAND CONTAMINATION
# =========================================================
cat("==========================================================\n")
cat("PART 1: MAINLAND CONTAMINATION\n")
cat("==========================================================\n\n")

# 1a. stateProvince breakdown
# Valid Galápagos records should mostly have stateProvince = "Galápagos"
# or similar.  A high count of other provinces signals contamination.
cat("--- 1a. stateProvince values (top 20) ---\n")
specimens %>%
  count(stateProvince, sort = TRUE) %>%
  slice_head(n = 20) %>%
  print(n = 20)
cat("\n")

# 1b. How many records have stateProvince that looks like Galápagos?
galapagos_province_pattern <- regex("gal[aá]pago", ignore_case = TRUE)
cat("--- 1b. Province matches 'galápago' vs other ---\n")
specimens %>%
  mutate(province_is_galapagos = str_detect(
    coalesce(stateProvince, ""), galapagos_province_pattern)) %>%
  count(province_is_galapagos) %>%
  print()
cat("\n")

# 1c. Which resolver assigned the island?
# 'best' matches 'latlon' → lat/lon-based (more reliable for real Galápagos coords)
# 'best' matches 'name'   → text-based (higher false-positive risk for mainland records)
# 'best' matches neither  → unusual, worth inspecting
cat("--- 1c. Resolver agreement (which resolver's answer matches 'best'?) ---\n")
specimens %>%
  mutate(
    best_from = case_when(
      best == latlon & best == name  ~ "both agree",
      best == latlon & best != name  ~ "latlon only",
      best != latlon & best == name  ~ "name only",
      TRUE                           ~ "neither (check)"
    )
  ) %>%
  count(best_from, sort = TRUE) %>%
  print()
cat("\n")

# 1d. For 'name only' resolutions: stateProvince breakdown
# These are the highest-risk records for mainland contamination.
cat("--- 1d. stateProvince for 'name only' resolved records (top 15) ---\n")
specimens %>%
  filter(best == name, best != latlon) %>%
  count(stateProvince, sort = TRUE) %>%
  slice_head(n = 15) %>%
  print(n = 15)
cat("\n")

# 1e. Sample of 'name only' records NOT from Galápagos province
cat("--- 1e. Sample of name-only records with non-Galápagos stateProvince ---\n")
specimens %>%
  filter(best == name, best != latlon) %>%
  filter(!str_detect(coalesce(stateProvince, ""), galapagos_province_pattern)) %>%
  select(gbifID, species, stateProvince, locality, best, name, latlon,
         decimalLongitude_std, decimalLatitude_std) %>%
  slice_sample(n = 20) %>%
  print(width = 120)
cat("\n")

# =========================================================
# PART 2: MISSING VERTEBRATE RECORDS
# =========================================================
cat("==========================================================\n")
cat("PART 2: MISSING VERTEBRATE RECORDS\n")
cat("==========================================================\n\n")

# 2a. class distribution across all galapagos_specimens
# Tells us what's present vs absent and whether class is populated
cat("--- 2a. 'class' field distribution in galapagos_specimens (all records) ---\n")
specimens %>%
  mutate(class_clean = coalesce(class, "(missing)")) %>%
  count(class_clean, sort = TRUE) %>%
  print(n = 30)
cat("\n")

# 2b. How many records in the target classes have no species name?
cat("--- 2b. Target class records with/without species name ---\n")
specimens %>%
  filter(class %in% TARGET_CLASSES) %>%
  mutate(has_species = !is.na(species) & species != "") %>%
  count(class, has_species) %>%
  pivot_wider(names_from = has_species, values_from = n,
              names_prefix = "has_species_") %>%
  rename(has_name = has_species_TRUE, no_name = has_species_FALSE) %>%
  print()
cat("\n")

# 2c. Spot-check: load full Ecuador input and check class field completeness
# (if the file is large this may take a minute)
cat("--- 2c. Checking class field in full Ecuador input (may take a moment)...\n")
full_input <- read_tsv(FULL_INPUT_FILE, col_types = cols(.default = col_character()),
                       show_col_types = FALSE)
cat(nrow(full_input), "total Ecuador records\n")

cat("\nTarget class counts in full Ecuador input:\n")
full_input %>%
  filter(class %in% TARGET_CLASSES) %>%
  count(class, basisOfRecord, sort = TRUE) %>%
  print(n = 40)

cat("\nRecords with target class that resolved to a Galápagos island:\n")
results <- read_tsv(RESULTS_FILE, col_types = cols(.default = col_character()),
                    show_col_types = FALSE)
full_input %>%
  filter(class %in% TARGET_CLASSES) %>%
  left_join(results %>% select(gbifID, best), by = "gbifID") %>%
  mutate(resolved = !is.na(best) & best != "-") %>%
  count(class, resolved) %>%
  pivot_wider(names_from = resolved, values_from = n,
              names_prefix = "resolved_") %>%
  rename(not_resolved = resolved_FALSE, resolved_to_island = resolved_TRUE) %>%
  print()
cat("\n")

# 2d. Among target-class records NOT resolved to an island — why not?
# Sample of unresolved records: what do their locality fields look like?
cat("--- 2d. Sample of unresolved target-class records (locality fields) ---\n")
full_input %>%
  filter(class %in% TARGET_CLASSES) %>%
  left_join(results %>% select(gbifID, best), by = "gbifID") %>%
  filter(is.na(best) | best == "-") %>%
  select(class, species, stateProvince, island, locality, verbatimLocality,
         decimalLatitude, decimalLongitude) %>%
  slice_sample(n = 30) %>%
  print(n = 30, width = 140)
cat("\n")

# 2e. Are there Galápagos-province records among the unresolved?
cat("--- 2e. Unresolved records where stateProvince suggests Galápagos ---\n")
full_input %>%
  filter(class %in% TARGET_CLASSES) %>%
  left_join(results %>% select(gbifID, best), by = "gbifID") %>%
  filter(is.na(best) | best == "-") %>%
  filter(str_detect(coalesce(stateProvince, ""), galapagos_province_pattern)) %>%
  count(class, stateProvince, sort = TRUE) %>%
  print(n = 20)

cat("\nSample of those records:\n")
full_input %>%
  filter(class %in% TARGET_CLASSES) %>%
  left_join(results %>% select(gbifID, best), by = "gbifID") %>%
  filter(is.na(best) | best == "-") %>%
  filter(str_detect(coalesce(stateProvince, ""), galapagos_province_pattern)) %>%
  select(class, species, stateProvince, island, locality, verbatimLocality,
         decimalLatitude, decimalLongitude) %>%
  slice_sample(n = 20) %>%
  print(n = 20, width = 140)

cat("\n\nDiagnostic complete.\n")

