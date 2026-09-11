# =========================================================
# Refine Galápagos Specimen Taxonomy
# =========================================================
# Prerequisite: run build_galapagos_thesaurus.R first.
#
# Applies a two-step taxonomic refinement to every record
# in the specimen files, adding two new columns:
#
#   accepted_name  -- the refined canonical species name
#   taxonomy_note  -- controlled vocabulary explaining what
#                     adjustment (if any) was applied
#   lookup_name    -- the join key used to query the
#                     thesaurus (useful for auditing)
#
# NO records are ever dropped.  When refinement is not
# possible, accepted_name falls back to the original name.
#
# ---- Two-step refinement logic -------------------------
#
# Step 1 -- Name-level resolution (via thesaurus):
#   The thesaurus maps each specimen name to a GBIF-
#   accepted canonical name, applying synonym resolution
#   and manual overrides from taxonomy.py.
#
# Step 2 -- Island-informed correction (via CDF ranges):
#   Using the CDF expected_islands data in the thesaurus,
#   checks whether the accepted name is expected on the
#   specimen's assigned island (best column).  If not,
#   attempts to find the species of the same genus that
#   IS expected there -- but only when exactly one such
#   species exists (to avoid guessing).
#   Also upgrades genus-only records to species level when
#   exactly one species of that genus is CDF-expected on
#   the island.
#
# ---- taxonomy_note controlled vocabulary ---------------
#
# Name-level results (no island correction applied):
#   "accepted"               Name already canonical; island
#                            within expected CDF range (or
#                            no CDF range data available)
#   "synonym_resolved"       GBIF backbone synonym resolved
#                            to accepted name
#   "manual_override"        Explicit override from the
#                            MANUAL_SYNONYMS in taxonomy.py
#   "not_in_thesaurus"       Name not found in thesaurus;
#                            accepted_name = original name
#   "no_name"                Record has no species-level or
#                            genus-level identification
#
# Island-informed corrections:
#   "genus_to_species_by_island"   Genus-only record assigned
#                            to species because exactly one
#                            species of that genus is CDF-
#                            expected on this island
#   "species_reassigned_by_island" Species name not expected
#                            on this island; reassigned to
#                            the single same-genus species
#                            the CDF does expect here
#
# Unresolvable situations (accepted_name unchanged):
#   "genus_ambiguous"        Genus-only; multiple same-genus
#                            species expected on island --
#                            cannot pick one
#   "genus_no_cdf_match"     Genus-only; genus absent from
#                            CDF for this island
#   "island_mismatch_ambiguous"  Species not expected on
#                            island; multiple same-genus
#                            alternatives exist -- ambiguous
#   "island_mismatch_unresolved" Species not expected on
#                            island; no same-genus species
#                            in CDF for this island
#   "class_not_targeted"     Class outside TARGET_CLASSES;
#                            no refinement attempted
#
# =========================================================

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(purrr)

# =========================================================
# CONFIG
# =========================================================

OUTPUT_DIR     <- "~/Dropbox/Galapagos_data/output/"
THESAURUS_FILE <- file.path(OUTPUT_DIR, "galapagos_thesaurus.tsv")

# Named character vector: input filename -> output filename
# All files are read from and written to OUTPUT_DIR.
SPECIMEN_FILES <- c(
  "galapagos_specimens.tsv"           = "galapagos_specimens_refined.tsv",
  "galapagos_specimens_multipull.tsv" = "galapagos_specimens_multipull_refined.tsv",
  "galapagos_gadm_specimens.tsv"      = "galapagos_gadm_specimens_refined.tsv"
)

# Classes for which refinement is attempted and stats reported
TARGET_CLASSES <- c("Aves", "Mammalia", "Testudines", "Squamata")

# =========================================================
# SECTION 1: LOAD THESAURUS
# =========================================================

cat("Section 1: Loading thesaurus...\n")

if (!file.exists(path.expand(THESAURUS_FILE))) {
  stop(
    "Thesaurus not found: ", THESAURUS_FILE, "\n",
    "Run build_galapagos_thesaurus.R first."
  )
}

thesaurus <- read_tsv(THESAURUS_FILE,
                      col_types      = cols(.default = col_character()),
                      show_col_types = FALSE) %>%
  mutate(override_applied = as.logical(override_applied))

cat(sprintf(
  "  %d thesaurus entries  |  %d classes  |  %d with CDF island data\n\n",
  nrow(thesaurus),
  n_distinct(thesaurus$class),
  sum(!is.na(thesaurus$expected_islands))
))

# =========================================================
# SECTION 2: BUILD GENUS x ISLAND LOOKUP
# =========================================================
# From the CDF expected_islands data already in the
# thesaurus, build a (genus, island) -> [candidate species]
# lookup.  This is the engine behind both genus-to-species
# upgrades and lateral island-informed reassignments.
#
# A lateral reassignment is only applied when n_candidates
# == 1: if two species of the same genus are expected on
# the island, we cannot pick between them without more data.

cat("Section 2: Building genus x island lookup from thesaurus...\n")

genus_island <- thesaurus %>%
  filter(!is.na(expected_islands), !is.na(accepted_name)) %>%
  mutate(genus = word(accepted_name, 1)) %>%
  select(genus, accepted_name, expected_islands) %>%
  distinct(accepted_name, .keep_all = TRUE) %>%
  # One row per species per island
  separate_rows(expected_islands, sep = "\\|") %>%
  filter(!is.na(expected_islands), expected_islands != "") %>%
  rename(island = expected_islands) %>%
  group_by(genus, island) %>%
  summarise(
    candidates   = list(sort(unique(accepted_name))),
    n_candidates = n(),
    .groups      = "drop"
  )

cat(sprintf(
  "  %d genera  x  %d islands  =  %d (genus, island) pairs\n\n",
  n_distinct(genus_island$genus),
  n_distinct(genus_island$island),
  nrow(genus_island)
))

# =========================================================
# SECTION 3: PROCESS EACH SPECIMEN FILE
# =========================================================

for (in_name in names(SPECIMEN_FILES)) {

  out_name <- SPECIMEN_FILES[[in_name]]
  in_path  <- path.expand(file.path(OUTPUT_DIR, in_name))
  out_path <- path.expand(file.path(OUTPUT_DIR, out_name))

  if (!file.exists(in_path)) {
    message("Skipping (not found): ", in_path)
    next
  }

  cat(sprintf("Processing: %s\n", in_name))
  cat(strrep("-", 50), "\n")

  specimens <- read_tsv(in_path,
                        col_types      = cols(.default = col_character()),
                        show_col_types = FALSE)
  cat(sprintf("  Loaded %d records.\n", nrow(specimens)))

  # Normalize class: GBIF sometimes classifies records as "Reptilia"
  # with order "Testudines" or "Squamata".  pipeline_class maps these
  # to the order name so they pass the TARGET_CLASSES filter.
  specimens <- specimens %>%
    mutate(
      pipeline_class = case_when(
        class == "Reptilia" & order == "Testudines" ~ "Testudines",
        class == "Reptilia" & order == "Squamata"   ~ "Squamata",
        TRUE                                         ~ class
      )
    )

  # ---- Step A: Compute lookup key ----------------------
  # Same priority order used in build_galapagos_thesaurus.R
  # Section 1 so that join keys align with thesaurus entries.
  #
  # Genus-only detection: GBIF's 'species' field is blank
  # when a record is not resolved to species level.  Use
  # the GBIF 'genus' column for the genus name; fall back
  # to the first word of scientificName.

  specimens <- specimens %>%
    mutate(
      lookup_name = case_when(
        !is.na(acceptedScientificName) & acceptedScientificName != "" ~
          acceptedScientificName,
        !is.na(species) & species != "" ~ species,
        !is.na(scientificName) & scientificName != "" ~ scientificName,
        TRUE ~ NA_character_
      ),
      is_genus_only = pipeline_class %in% TARGET_CLASSES &
                      (is.na(species) | species == "") &
                      !is.na(coalesce(genus, word(scientificName, 1))),
      lookup_genus  = if_else(
        is_genus_only,
        coalesce(genus, word(scientificName, 1)),
        NA_character_
      )
    )

  target_recs <- sum(specimens$pipeline_class %in% TARGET_CLASSES, na.rm = TRUE)
  genus_recs  <- sum(specimens$is_genus_only, na.rm = TRUE)
  cat(sprintf("  Target-class records: %d  |  Genus-only: %d\n",
              target_recs, genus_recs))

  # ---- Step B: Join thesaurus --------------------------
  # Brings in thes_accepted (accepted name), override flag,
  # and CDF expected_islands for island range checking.

  specimens <- specimens %>%
    left_join(
      thesaurus %>%
        select(original_name,
               thes_accepted    = accepted_name,
               override_applied,
               expected_islands),
      by = c("lookup_name" = "original_name")
    )

  # ---- Step C: Check island membership ----------------
  # For each record, is best within the CDF expected range?
  # NA  -> cannot check (no CDF range data, or best is NA)
  # TRUE  -> island confirmed within expected range
  # FALSE -> island NOT in expected range: possible issue

  specimens <- specimens %>%
    mutate(
      island_expected = map2_lgl(
        best, expected_islands,
        function(b, ei) {
          if (is.na(b) || b == "-" || is.na(ei)) return(NA)
          b %in% str_split(ei, fixed("|"))[[1]]
        }
      )
    )

  # ---- Step D: Join genus x island lookup --------------
  # For species-level records: join on (genus of accepted
  # name, best island) to find alternatives for a lateral
  # reassignment.
  # For genus-only records: join on (lookup_genus, best
  # island) to find the single expected species if any.

  specimens <- specimens %>%
    mutate(
      join_genus = case_when(
        is_genus_only          ~ lookup_genus,
        !is.na(thes_accepted)  ~ word(thes_accepted, 1),
        !is.na(lookup_name)    ~ word(lookup_name,   1),
        TRUE                   ~ NA_character_
      )
    ) %>%
    left_join(
      genus_island,
      by = c("join_genus" = "genus", "best" = "island")
    )

  # Safely extract first (and only) candidate when n == 1
  specimens <- specimens %>%
    mutate(
      sole_candidate = map_chr(
        candidates,
        ~ if (is.null(.x) || length(.x) == 0) NA_character_ else .x[[1]]
      )
    )

  # ---- Step E: Derive accepted_name and taxonomy_note --

  specimens <- specimens %>%
    mutate(

      # Base note from name-level resolution alone
      name_note = case_when(
        !pipeline_class %in% TARGET_CLASSES      ~ "class_not_targeted",
        is.na(lookup_name)                      ~ "no_name",
        is.na(thes_accepted)                    ~ "not_in_thesaurus",
        coalesce(override_applied, FALSE)       ~ "manual_override",
        thes_accepted != lookup_name            ~ "synonym_resolved",
        TRUE                                    ~ "accepted"
      ),

      # Final accepted name
      accepted_name = case_when(
        # Non-target classes: preserve original lookup name unchanged
        !pipeline_class %in% TARGET_CLASSES ~ coalesce(lookup_name, NA_character_),
        # Genus-only: upgrade to species if exactly 1 expected
        is_genus_only & n_candidates == 1 ~ sole_candidate,
        # Genus-only unresolved: store as "Genus sp." for clarity
        is_genus_only & !is.na(lookup_genus) ~ paste(lookup_genus, "sp."),
        # Island mismatch: lateral reassignment if exactly 1 option
        isFALSE(island_expected) & n_candidates == 1 ~ sole_candidate,
        # All other cases: use thesaurus accepted name
        !is.na(thes_accepted) ~ thes_accepted,
        # Last resort: original lookup name unchanged
        TRUE ~ lookup_name
      ),

      # Island-adjusted taxonomy note
      # Island corrections override the name-level base note
      # when a meaningful change has actually been made.
      taxonomy_note = case_when(
        # Non-target and no-name cases pass through unchanged
        name_note %in% c("class_not_targeted", "no_name") ~ name_note,
        # Genus-only: outcome depends on genus x island lookup
        is_genus_only & is.na(n_candidates)   ~ "genus_no_cdf_match",
        is_genus_only & n_candidates  > 1     ~ "genus_ambiguous",
        is_genus_only & n_candidates == 1     ~ "genus_to_species_by_island",
        # Name not in thesaurus: no island validation possible
        name_note == "not_in_thesaurus"        ~ "not_in_thesaurus",
        # Island expected (or unknown): report name-level result
        is.na(island_expected)                 ~ name_note,
        isTRUE(island_expected)                ~ name_note,
        # Island mismatch (island_expected == FALSE)
        is.na(n_candidates)    ~ "island_mismatch_unresolved",
        n_candidates  > 1      ~ "island_mismatch_ambiguous",
        n_candidates == 1      ~ "species_reassigned_by_island",
        TRUE                   ~ name_note
      )
    ) %>%
    # Drop working columns; keep lookup_name for audit trail
    select(-thes_accepted, -override_applied, -expected_islands,
           -island_expected, -is_genus_only, -lookup_genus,
           -join_genus, -candidates, -n_candidates,
           -sole_candidate, -name_note)

  # ---- Step F: Summary ---------------------------------

  cat("\n  taxonomy_note breakdown (target classes only):\n")
  note_summary <- specimens %>%
    filter(pipeline_class %in% TARGET_CLASSES) %>%
    count(taxonomy_note, sort = TRUE) %>%
    mutate(pct = sprintf("%5.1f%%", 100 * n / sum(n)))
  print(note_summary, n = 20)

  n_lateral  <- sum(specimens$taxonomy_note == "species_reassigned_by_island", na.rm = TRUE)
  n_upgrade  <- sum(specimens$taxonomy_note == "genus_to_species_by_island",   na.rm = TRUE)
  n_mismatch <- sum(specimens$taxonomy_note %in%
                      c("island_mismatch_unresolved", "island_mismatch_ambiguous"),
                    na.rm = TRUE)

  cat(sprintf(
    "\n  Genus -> species upgrades      : %d\n",   n_upgrade))
  cat(sprintf(
    "  Lateral island reassignments   : %d\n",   n_lateral))
  cat(sprintf(
    "  Island mismatches (unresolved) : %d\n\n", n_mismatch))

  if (n_lateral > 0) {
    cat("  Sample of lateral reassignments:\n")
    specimens %>%
      filter(taxonomy_note == "species_reassigned_by_island") %>%
      select(lookup_name, accepted_name, best, class) %>%
      distinct(lookup_name, accepted_name, best, .keep_all = TRUE) %>%
      arrange(class, lookup_name, best) %>%
      print(n = 20)
    cat("\n")
  }

  if (n_mismatch > 0) {
    cat("  Island mismatches not resolved (review recommended):\n")
    specimens %>%
      filter(taxonomy_note %in%
               c("island_mismatch_unresolved", "island_mismatch_ambiguous")) %>%
      count(accepted_name, best, taxonomy_note, sort = TRUE) %>%
      print(n = 20)
    cat("\n")
  }

  # ---- Step G: Write output ----------------------------
  write_tsv(specimens, out_path)
  cat(sprintf("  Written: %s\n", basename(out_path)))
  cat(sprintf("  Total records: %d  (no records dropped)\n\n",
              nrow(specimens)))
}

# =========================================================
# DONE
# =========================================================

cat(strrep("=", 55), "\n")
cat("Done.\n\n")
cat("New columns in refined output files:\n")
cat("  accepted_name  -- refined canonical species name\n")
cat("  taxonomy_note  -- type of adjustment applied\n")
cat("  lookup_name    -- thesaurus join key (audit trail)\n\n")
cat("Next step: update species_by_island.R to join on\n")
cat("accepted_name instead of species_name.\n")
