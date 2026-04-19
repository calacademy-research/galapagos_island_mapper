# =========================================================
# Galápagos Terrestrial Vertebrates: Species × Island Tables
# =========================================================
# Reads the filtered Galápagos specimens produced by
# gbif_ecuador_download.R and builds two summary tables for
# each of four vertebrate classes:
#
#   <class>_record_counts.tsv  — number of specimens per
#                                species per island
#   <class>_last_year.tsv      — most recent year a specimen
#                                was collected per species
#                                per island
#
# In both tables: rows = species, columns = islands, with an
# extra "archipelago" column on the right for records that
# are confirmed Galápagos (stateProvince) but could not be
# assigned to a specific island by analyze.py.
# =========================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)

# =========================================================
# CONFIG
# =========================================================

INPUT_FILE       <- "~/Dropbox/Galapagos_data/output/galapagos_specimens.tsv"
UNRESOLVED_FILE  <- "~/Dropbox/Galapagos_data/output/galapagos_unresolved.tsv"
OUTPUT_DIR       <- "~/Dropbox/Galapagos_data/output/species_by_island/"

# Terrestrial vertebrate classes to summarize
TARGET_CLASSES <- c("Aves", "Mammalia", "Testudines", "Squamata")

# Drop records with no species name
REQUIRE_SPECIES <- TRUE

# =========================================================
# SECTION 1: LOAD AND PREPARE DATA
# =========================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ── Island-resolved specimens ─────────────────────────────
specimens <- read_tsv(
  INPUT_FILE,
  col_types      = cols(.default = col_character()),
  show_col_types = FALSE
)
cat("Loaded", nrow(specimens), "island-resolved Galápagos specimen records\n")

# ── Unresolved but confirmed Galápagos records ────────────
# These have stateProvince = Galápagos but analyze.py could
# not place them on a specific island.  We add them as an
# "archipelago" column in the output tables.
unresolved_raw <- read_tsv(
  UNRESOLVED_FILE,
  col_types      = cols(.default = col_character()),
  show_col_types = FALSE
)
cat("Loaded", nrow(unresolved_raw), "unresolved Galápagos records\n\n")

# Shared helper: resolve best species name from available fields
add_species_name <- function(df) {
  df %>%
    mutate(
      species_name = case_when(
        !is.na(species)                & species                != "" ~ species,
        !is.na(acceptedScientificName) & acceptedScientificName != "" ~ acceptedScientificName,
        !is.na(scientificName)         & scientificName         != "" ~ scientificName,
        TRUE ~ NA_character_
      ),
      year_num = suppressWarnings(as.integer(year))
    )
}

specimens   <- add_species_name(specimens)
unresolved  <- add_species_name(unresolved_raw)

# Filter both to target vertebrate classes
vertebrates  <- specimens  %>% filter(class %in% TARGET_CLASSES)
unres_verts  <- unresolved %>% filter(class %in% TARGET_CLASSES)

cat(sprintf("Island-resolved vertebrate records : %d\n", nrow(vertebrates)))
cat(sprintf("Unresolved vertebrate records      : %d\n\n", nrow(unres_verts)))

# =========================================================
# SECTION 2: HELPERS
# =========================================================

# Build a wide species × island matrix from a long summary.
# fill_val fills cells where the species was not recorded on an island.
make_matrix <- function(long_df, value_col, fill_val) {
  long_df %>%
    pivot_wider(
      names_from  = best,
      values_from = !!sym(value_col),
      values_fill = fill_val
    ) %>%
    arrange(species_name)
}

# Append an "archipelago" column to a wide matrix.
# arch_df must have columns: species_name + the value column.
append_archipelago <- function(wide_df, arch_df, value_col) {
  left_join(wide_df, arch_df, by = "species_name") %>%
    rename(archipelago = !!sym(value_col))
}

# =========================================================
# SECTION 3: BUILD AND WRITE TABLES FOR EACH CLASS
# =========================================================

for (cls in TARGET_CLASSES) {

  cat("Processing:", cls, "\n")

  # ── Island-resolved data for this class ──────────────────
  class_data <- vertebrates %>% filter(class == cls)

  if (REQUIRE_SPECIES) {
    n_before   <- nrow(class_data)
    class_data <- class_data %>% filter(!is.na(species_name))
    n_dropped  <- n_before - nrow(class_data)
    if (n_dropped > 0)
      message(sprintf("  %s: dropped %d island-resolved records with no species name", cls, n_dropped))
  }

  # ── Unresolved (archipelago-level) data for this class ───
  arch_data <- unres_verts %>% filter(class == cls)

  if (REQUIRE_SPECIES)
    arch_data <- arch_data %>% filter(!is.na(species_name))

  if (nrow(class_data) == 0 && nrow(arch_data) == 0) {
    message(sprintf("  %s: no records after filtering — skipping", cls))
    next
  }

  cat(sprintf(
    "  %d island-resolved records, %d species, %d islands\n  %d archipelago-only records, %d species\n",
    nrow(class_data), n_distinct(class_data$species_name), n_distinct(class_data$best),
    nrow(arch_data),  n_distinct(arch_data$species_name)
  ))

  # ── Table 1: record counts ──────────────────────────────

  counts_long <- class_data %>%
    group_by(species_name, best) %>%
    summarise(n_records = n(), .groups = "drop")

  counts_wide <- make_matrix(counts_long, "n_records", fill_val = 0L)

  # Archipelago column: total count of unresolved records per species
  arch_counts <- arch_data %>%
    group_by(species_name) %>%
    summarise(n_records = n(), .groups = "drop")

  counts_wide <- append_archipelago(counts_wide, arch_counts, "n_records")

  out_counts <- file.path(OUTPUT_DIR, paste0(tolower(cls), "_record_counts.tsv"))
  write_tsv(counts_wide, out_counts)
  cat("  Written:", out_counts, "\n")

  # ── Table 2: most recent year ───────────────────────────

  year_long <- class_data %>%
    filter(!is.na(year_num)) %>%
    group_by(species_name, best) %>%
    summarise(last_year = max(year_num), .groups = "drop")

  year_wide <- make_matrix(year_long, "last_year", fill_val = NA_integer_)

  # Archipelago column: most recent year among unresolved records
  arch_years <- arch_data %>%
    filter(!is.na(year_num)) %>%
    group_by(species_name) %>%
    summarise(last_year = max(year_num), .groups = "drop")

  year_wide <- append_archipelago(year_wide, arch_years, "last_year")

  out_year <- file.path(OUTPUT_DIR, paste0(tolower(cls), "_last_year.tsv"))
  write_tsv(year_wide, out_year)
  cat("  Written:", out_year, "\n\n")

}

cat("Done.  Output files are in:", OUTPUT_DIR, "\n")
