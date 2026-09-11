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
# In both tables: rows = species sorted in taxonomic order
# (taxon_order → family → species_name); columns = islands,
# with an extra "archipelago" column on the right for records
# that are confirmed Galápagos (stateProvince) but could not
# be assigned to a specific island by analyze.py.  The first
# three columns are species_name, taxon_order, and family.
#
# ── Thesaurus integration (USE_REFINED = TRUE) ───────────
# When USE_REFINED = TRUE the script reads the output of
# refine_taxonomy.R (*_refined.tsv), which adds two columns:
#
#   accepted_name   — canonical name after synonym resolution
#                     and island-informed corrections
#   taxonomy_note   — controlled vocab flag for each record
#
# Row labels in the output tables are then accepted_name
# rather than the raw GBIF species string.  Key benefits:
#
#   • Synonyms are collapsed: records filed under old names
#     (e.g. "Nesomimus parvulus") count toward the accepted
#     species ("Mimus parvulus")
#   • Island corrections: a record of "Mimus trifasciatus"
#     from Española goes into the M. macdonaldi row
#   • Genus upgrades: "Mimus sp." from Española is counted
#     under M. macdonaldi when it is the only option
#
# Set USE_REFINED = FALSE to reproduce the pre-thesaurus
# output using raw GBIF species names.
#
# Unresolved records (galapagos_unresolved.tsv) are always
# joined to the thesaurus for name-level synonym resolution
# when THESAURUS_FILE is available, even when USE_REFINED is
# FALSE (island-informed corrections cannot apply to records
# without a specific island assignment).
# =========================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)

# =========================================================
# CONFIG
# =========================================================

OUTPUT_DIR       <- "~/Dropbox/Galapagos_data/output/"
UNRESOLVED_FILE  <- file.path(OUTPUT_DIR, "galapagos_unresolved.tsv")
THESAURUS_FILE   <- file.path(OUTPUT_DIR, "galapagos_thesaurus.tsv")
SPECIES_OUT_DIR  <- file.path(OUTPUT_DIR, "species_by_island/")

# ── Input file selection ──────────────────────────────────
# USE_REFINED = TRUE  → read refined specimens (accepted_name
#                       already populated by refine_taxonomy.R)
# USE_REFINED = FALSE → read raw specimens (pre-thesaurus
#                       behaviour; species_name from GBIF fields)
USE_REFINED    <- TRUE
REFINED_FILE   <- file.path(OUTPUT_DIR, "galapagos_specimens_refined.tsv")
RAW_FILE       <- file.path(OUTPUT_DIR, "galapagos_specimens.tsv")

# Terrestrial vertebrate classes to summarize
TARGET_CLASSES <- c("Aves", "Mammalia", "Testudines", "Squamata")

# Drop records with no species name
REQUIRE_SPECIES <- TRUE

# =========================================================
# SECTION 1: LOAD AND PREPARE DATA
# =========================================================

dir.create(path.expand(SPECIES_OUT_DIR), recursive = TRUE,
           showWarnings = FALSE)

# ── Load thesaurus for unresolved-record name resolution ──
# Even when USE_REFINED = FALSE, the thesaurus is used to
# resolve synonyms in the unresolved (archipelago) records.
thesaurus_available <- file.exists(path.expand(THESAURUS_FILE))
if (thesaurus_available) {
  thesaurus_names <- read_tsv(
    THESAURUS_FILE,
    col_types      = cols(original_name = col_character(),
                          accepted_name  = col_character(),
                          .default       = col_skip()),
    show_col_types = FALSE
  )
  cat(sprintf("Loaded thesaurus: %d name mappings.\n", nrow(thesaurus_names)))
} else {
  message("Thesaurus not found at: ", THESAURUS_FILE)
  message("Run build_galapagos_thesaurus.R to enable synonym resolution.")
  thesaurus_names <- tibble(original_name = character(),
                            accepted_name  = character())
}

# ── Island-resolved specimens ─────────────────────────────
input_path <- if (USE_REFINED &&
                  file.exists(path.expand(REFINED_FILE))) {
  cat("Using refined specimens file (USE_REFINED = TRUE).\n")
  REFINED_FILE
} else {
  if (USE_REFINED)
    message("Refined file not found -- falling back to raw specimens.\n",
            "Run refine_taxonomy.R first to enable thesaurus integration.")
  RAW_FILE
}

specimens_raw <- read_tsv(
  input_path,
  col_types      = cols(.default = col_character()),
  show_col_types = FALSE
)
cat(sprintf("Loaded %d island-resolved specimen records from %s\n",
            nrow(specimens_raw), basename(input_path)))

# ── Unresolved but confirmed Galápagos records ────────────
# These have stateProvince = Galápagos but analyze.py could
# not place them on a specific island.  They form the
# "archipelago" column in the output tables.
unresolved_raw <- read_tsv(
  UNRESOLVED_FILE,
  col_types      = cols(.default = col_character()),
  show_col_types = FALSE
)
cat(sprintf("Loaded %d unresolved Galápagos records\n\n",
            nrow(unresolved_raw)))

# ── Helper: raw species name from GBIF fields ─────────────
# Priority: species > acceptedScientificName > scientificName
# Used for the unresolved file and as fallback when the
# refined file is absent.
derive_raw_name <- function(df) {
  df %>%
    mutate(
      raw_name = case_when(
        !is.na(species)                & species                != "" ~ species,
        !is.na(acceptedScientificName) & acceptedScientificName != "" ~ acceptedScientificName,
        !is.na(scientificName)         & scientificName         != "" ~ scientificName,
        TRUE ~ NA_character_
      ),
      # Lookup key matching the thesaurus build priority order:
      # acceptedScientificName > species > scientificName
      lookup_name = case_when(
        !is.na(acceptedScientificName) & acceptedScientificName != "" ~ acceptedScientificName,
        !is.na(species)                & species                != "" ~ species,
        !is.na(scientificName)         & scientificName         != "" ~ scientificName,
        TRUE ~ NA_character_
      ),
      year_num = suppressWarnings(as.integer(year))
    )
}

# ── Prepare island-resolved specimens ─────────────────────
if ("accepted_name" %in% names(specimens_raw) &&
    "taxonomy_note" %in% names(specimens_raw)) {
  # Refined file: accepted_name and taxonomy_note already present
  specimens <- specimens_raw %>%
    mutate(
      species_name = coalesce(accepted_name,
                              species,
                              acceptedScientificName,
                              scientificName),
      year_num     = suppressWarnings(as.integer(year)),
      pipeline_class = case_when(
        class == "Reptilia" & order == "Testudines" ~ "Testudines",
        class == "Reptilia" & order == "Squamata"   ~ "Squamata",
        TRUE                                         ~ class
      )
    )
  cat("taxonomy_note breakdown for island-resolved vertebrates:\n")
  specimens %>%
    filter(pipeline_class %in% TARGET_CLASSES) %>%
    count(taxonomy_note, sort = TRUE) %>%
    mutate(pct = sprintf("%5.1f%%", 100 * n / sum(n))) %>%
    print()
  cat("\n")
} else {
  # Raw file: derive species_name from GBIF fields
  specimens <- specimens_raw %>%
    derive_raw_name() %>%
    mutate(
      species_name = raw_name,
      pipeline_class = case_when(
        class == "Reptilia" & order == "Testudines" ~ "Testudines",
        class == "Reptilia" & order == "Squamata"   ~ "Squamata",
        TRUE                                         ~ class
      )
    )
}

# ── Prepare unresolved records ────────────────────────────
# Apply name-level thesaurus resolution (synonym collapsing)
# even though island-informed corrections cannot apply here.
unresolved <- unresolved_raw %>%
  derive_raw_name() %>%
  left_join(thesaurus_names,
            by = c("lookup_name" = "original_name")) %>%
  mutate(
    # Use thesaurus accepted_name when available; fall back to raw
    species_name = coalesce(accepted_name, raw_name),
    pipeline_class = case_when(
      class == "Reptilia" & order == "Testudines" ~ "Testudines",
      class == "Reptilia" & order == "Squamata"   ~ "Squamata",
      TRUE                                         ~ class
    )
  ) %>%
  select(-raw_name, -lookup_name, -accepted_name)

# ── Diagnostic: unexpected best values in specimens ───────
# galapagos_specimens*.tsv should only contain records with a
# specific island in 'best'.  Any NA / empty / "-" values here
# indicate a gap in the upstream filter.
bad_best <- specimens %>%
  filter(is.na(best) | best == "" | best == "-")

if (nrow(bad_best) > 0) {
  cat(sprintf(
    "\nWARNING: %d specimen record(s) have missing/empty/'-' best value.\n",
    nrow(bad_best)
  ))
  cat("Diagnosing which filter conditions they satisfy:\n")
  GALAPAGOS_PATTERN      <- regex("al[aá]?pag", ignore_case = TRUE)
  ENGLISH_ISLAND_PATTERN <- regex(
    paste("\\balbemarle\\b", "\\bnarborough\\b", "\\bindefatigable\\b",
          "\\bchatham island\\b", "\\bcharles island\\b", "\\bjames island\\b",
          "\\btower island\\b", "\\bbindloe\\b", "\\babingdon\\b",
          "\\bjervis island\\b", "\\bbarrington island\\b", "\\bculpepper\\b",
          "\\bwenman\\b", "\\bnorth seymour\\b", "\\bsouth seymour\\b",
          "\\bduncan island\\b", sep = "|"),
    ignore_case = TRUE
  )
  bad_best %>%
    mutate(
      .A_latlon  = !is.na(latlon) & latlon != "-",
      .B_gadm    = coalesce(level1Gid, "") == "ECU.9_1",
      .C_prov    = str_detect(coalesce(stateProvince, ""), GALAPAGOS_PATTERN),
      .D_loc     = str_detect(
        paste(coalesce(locality,""), coalesce(verbatimLocality,""),
              coalesce(island,""),   coalesce(islandGroup,""),
              coalesce(county,""),   coalesce(occurrenceRemarks,""),
              coalesce(locationRemarks,""), sep=" "),
        GALAPAGOS_PATTERN),
      .E_english = str_detect(
        paste(coalesce(locality,""), coalesce(verbatimLocality,""),
              coalesce(island,""), sep=" "),
        ENGLISH_ISLAND_PATTERN)
    ) %>%
    select(gbifID, species, stateProvince, locality, latlon,
           .A_latlon, .B_gadm, .C_prov, .D_loc, .E_english) %>%
    print(n = 20)
} else {
  cat("\nAll specimen records have a valid island in 'best'. Good.\n")
}

# Filter to target vertebrate classes.
# Also explicitly exclude any island-resolved records with a missing or
# placeholder 'best' value — these should not exist (gbif_ecuador_download.R
# Step 1 already guarantees a valid 'best' in galapagos_specimens.tsv), but
# the filter is kept as a defensive guard against stale or mixed input files.
# NOTE: unres_verts intentionally retains records where best is NA / "-";
# they are the source of the "archipelago" column in the output tables.
vertebrates  <- specimens  %>%
  filter(pipeline_class %in% TARGET_CLASSES) %>%
  filter(!is.na(best) & best != "" & best != "-")
unres_verts  <- unresolved %>% filter(pipeline_class %in% TARGET_CLASSES)

cat(sprintf("Island-resolved vertebrate records : %d\n", nrow(vertebrates)))
cat(sprintf("Unresolved vertebrate records      : %d\n\n", nrow(unres_verts)))

# =========================================================
# SECTION 2: HELPERS
# =========================================================

# Build a wide species × island matrix from a long summary.
# fill_val fills cells where the species was not recorded on an island.
# Row order is left to the caller (taxonomic sort applied after joining
# the taxon_order_lookup below).
make_matrix <- function(long_df, value_col, fill_val) {
  long_df %>%
    pivot_wider(
      names_from  = best,
      values_from = !!sym(value_col),
      values_fill = fill_val
    )
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
  class_data <- vertebrates %>% filter(pipeline_class == cls)

  if (REQUIRE_SPECIES) {
    n_before   <- nrow(class_data)
    class_data <- class_data %>% filter(!is.na(species_name))
    n_dropped  <- n_before - nrow(class_data)
    if (n_dropped > 0)
      message(sprintf(
        "  %s: dropped %d island-resolved records with no species name",
        cls, n_dropped))
  }

  # ── Unresolved (archipelago-level) data for this class ───
  arch_data <- unres_verts %>% filter(pipeline_class == cls)

  if (REQUIRE_SPECIES)
    arch_data <- arch_data %>% filter(!is.na(species_name))

  if (nrow(class_data) == 0 && nrow(arch_data) == 0) {
    message(sprintf("  %s: no records after filtering — skipping", cls))
    next
  }

  cat(sprintf(
    "  %d island-resolved records, %d species, %d islands\n  %d archipelago-only records, %d species\n",
    nrow(class_data), n_distinct(class_data$species_name),
    n_distinct(class_data$best),
    nrow(arch_data),  n_distinct(arch_data$species_name)
  ))

  # ── Taxonomic sort-order lookup ───────────────────────────
  # Derived from both island-resolved and archipelago records so that
  # species appearing only in the "archipelago" column are also covered.
  # GBIF's 'order' column is renamed to 'taxon_order' to avoid shadowing
  # base::order().  Where a name maps to multiple GBIF orders/families
  # (data-entry inconsistencies) the first occurrence is kept.
  taxon_order_lookup <- bind_rows(
    class_data %>% select(species_name, taxon_order = order, family),
    arch_data  %>% select(species_name, taxon_order = order, family)
  ) %>%
    filter(!is.na(species_name)) %>%
    distinct(species_name, .keep_all = TRUE)

  # ── Table 1: record counts ──────────────────────────────

  counts_long <- class_data %>%
    group_by(species_name, best) %>%
    summarise(n_records = n(), .groups = "drop")

  counts_wide <- make_matrix(counts_long, "n_records", fill_val = 0L)

  # Archipelago column: total count of unresolved records per species
  arch_counts <- arch_data %>%
    group_by(species_name) %>%
    summarise(n_records = n(), .groups = "drop")

  counts_wide <- append_archipelago(counts_wide, arch_counts, "n_records") %>%
    left_join(taxon_order_lookup, by = "species_name") %>%
    arrange(taxon_order, family, species_name) %>%
    select(species_name, taxon_order, family, everything())

  out_counts <- file.path(SPECIES_OUT_DIR,
                           paste0(tolower(cls), "_record_counts.tsv"))
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

  year_wide <- append_archipelago(year_wide, arch_years, "last_year") %>%
    left_join(taxon_order_lookup, by = "species_name") %>%
    arrange(taxon_order, family, species_name) %>%
    select(species_name, taxon_order, family, everything())

  out_year <- file.path(SPECIES_OUT_DIR,
                         paste0(tolower(cls), "_last_year.tsv"))
  write_tsv(year_wide, out_year)
  cat("  Written:", out_year, "\n\n")

}

cat("Done.  Output files are in:", SPECIES_OUT_DIR, "\n")
if (USE_REFINED)
  cat("Row labels are accepted_name from the taxonomic thesaurus.\n")
