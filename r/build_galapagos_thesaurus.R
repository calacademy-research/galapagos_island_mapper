# =========================================================
# Build Galápagos Taxonomic Thesaurus
# =========================================================
# Constructs a species-level lookup table for Galápagos
# taxa by:
#
#   1. Collecting every unique species name that appears in
#      the pipeline's specimen output files
#   2. Resolving each name against the GBIF taxonomic
#      backbone (via name_backbone_checklist) to normalise
#      synonyms → accepted names and flag doubtful records
#   3. Cross-referencing the IOC World Bird List (already
#      in data/ioc-names-14.1.xml) for bird-specific
#      canonical ordering
#   4. Joining against the CDF Galápagos species checklist
#      (data/cdf_galapagos_checklist.tsv — see note below)
#      to add Galápagos-specific status and island ranges
#   5. Incorporating the small manual synonym overrides
#      already maintained in src/taxonomy.py
#   6. Writing galapagos_thesaurus.tsv for use by
#      species_by_island.R and future downstream scripts
#
# ── About the CDF checklist file ────────────────────────
# The Charles Darwin Foundation maintains a Galápagos
# Species Database at:
#   https://www.darwinfoundation.org/en/datazone/checklist
#
# Download the checklist for the taxa of interest (birds,
# reptiles, mammals) and save as:
#   data/cdf_galapagos_checklist.tsv
#
# Expected columns (rename as needed; see CONFIG below):
#   cdf_name         — scientific name as CDF uses it
#   galapagos_status — endemic / native / introduced /
#                      vagrant / extirpated / etc.
#   expected_islands — pipe-separated island list
#                      e.g. "santa cruz|isabela|fernandina"
#   common_name      — optional
#   notes            — optional
#
# The script runs without this file (it degrades gracefully
# to GBIF-only output) but Galápagos-status columns will
# be NA until the file is provided.
#
# ── GBIF API caching ────────────────────────────────────
# name_backbone_checklist() queries the GBIF API once per
# unique name. Results are cached to
#   ~/Dropbox/Galapagos_data/output/gbif_backbone_cache.tsv
# so subsequent runs only query new or changed names.
# Delete the cache file to force a full re-query.
# =========================================================

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(rgbif)    # name_backbone_checklist()
library(xml2)     # reading ioc-names-14.1.xml

# =========================================================
# CONFIG
# =========================================================

OUTPUT_DIR     <- "~/Dropbox/Galapagos_data/output/"
THESAURUS_FILE <- file.path(OUTPUT_DIR, "galapagos_thesaurus.tsv")
CACHE_FILE     <- file.path(OUTPUT_DIR, "gbif_backbone_cache.tsv")

# Specimen files to harvest names from (all pipelines)
SPECIMEN_FILES <- c(
  file.path(OUTPUT_DIR, "galapagos_specimens.tsv"),
  file.path(OUTPUT_DIR, "galapagos_specimens_multipull.tsv"),
  file.path(OUTPUT_DIR, "galapagos_gadm_specimens.tsv")
)

# Reference files (in repo)
REPO_ROOT  <- path.expand("~/galapagos_island_mapper")
IOC_FILE   <- file.path(REPO_ROOT, "data", "ioc-names-14.1.xml")
CDF_FILE   <- file.path(REPO_ROOT, "data", "cdf_galapagos_checklist.tsv")

# ── CDF column name mapping ───────────────────────────────
# Adjust these to match the actual column names in your
# downloaded CDF file if they differ.
CDF_COL_NAME    <- "cdf_name"          # scientific name
CDF_COL_STATUS  <- "galapagos_status"  # endemic / native / etc.
CDF_COL_ISLANDS <- "expected_islands"  # pipe-separated island list
CDF_COL_COMMON  <- "common_name"       # optional
CDF_COL_NOTES   <- "notes"             # optional

# Vertebrate classes to include
TARGET_CLASSES <- c("Aves", "Mammalia", "Testudines", "Squamata")

# Manual synonym overrides from src/taxonomy.py.
# These are bird names known to appear in GBIF data that
# map to the IOC-accepted name.  Keep this in sync with
# the synonyms dict in taxonomy.py.
MANUAL_SYNONYMS <- c(
  "Oceanodroma castro"    = "Hydrobates castro",
  "Aphriza virgata"       = "Calidris virgata",
  "Oceanodroma leucorhoa" = "Hydrobates leucorhous",
  "Phalacrocorax harrisi" = "Nannopterum harrisi",
  "Puffinus creatopus"    = "Ardenna creatopus",
  "Philomachus pugnax"    = "Calidris pugnax",
  "Anas clypeata"         = "Spatula clypeata",
  "Anas cyanoptera"       = "Spatula cyanoptera",
  "Aratinga erythrogenys" = "Psittacara erythrogenys",
  "Anas discors"          = "Spatula discors",
  "Puffinus pacificus"    = "Ardenna pacifica",
  "Puffinus griseus"      = "Ardenna grisea",
  "Charadrius wilsonia"   = "Anarhynchus wilsonia",
  "Tryngites subruficollis" = "Calidris subruficollis",
  "Oceanodroma tethys"    = "Hydrobates tethys",
  "Laterallus spilonotus" = "Laterallus spilonota",
  "Neocrex erythrops"     = "Mustelirallus erythrops",
  "Oceanodroma markhami"  = "Hydrobates markhami",
  "Oceanodroma hornbyi"   = "Hydrobates hornbyi",
  "Oceanodroma microsoma" = "Hydrobates microsoma"
)

# =========================================================
# SECTION 1: COLLECT UNIQUE SPECIES NAMES
# =========================================================

cat("Section 1: Collecting unique species names from specimen files...\n")

all_names <- purrr::map_dfr(SPECIMEN_FILES, function(f) {
  if (!file.exists(f)) {
    message("  Skipping (not found): ", f)
    return(tibble())
  }
  cat("  Reading:", basename(f), "\n")
  read_tsv(f,
           col_types      = cols(.default = col_character()),
           show_col_types = FALSE) %>%
    filter(class %in% TARGET_CLASSES) %>%
    select(class,
           species_in_data  = species,
           accepted_in_gbif = acceptedScientificName,
           verbatim_name    = scientificName) %>%
    distinct()
})

# Build the set of unique names to resolve.
# We prefer acceptedScientificName (GBIF has already done
# some backbone resolution), falling back to species, then
# scientificName.
names_to_resolve <- all_names %>%
  mutate(
    query_name = case_when(
      !is.na(accepted_in_gbif) & accepted_in_gbif != "" ~ accepted_in_gbif,
      !is.na(species_in_data)  & species_in_data  != "" ~ species_in_data,
      !is.na(verbatim_name)    & verbatim_name    != "" ~ verbatim_name,
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(query_name)) %>%
  select(class, query_name, species_in_data) %>%
  distinct(query_name, .keep_all = TRUE)

cat(sprintf(
  "  Found %d unique names across %d classes.\n\n",
  nrow(names_to_resolve),
  n_distinct(names_to_resolve$class)
))

# =========================================================
# SECTION 2: RESOLVE AGAINST GBIF BACKBONE (with caching)
# =========================================================

cat("Section 2: Resolving names against GBIF taxonomic backbone...\n")

# Load cache if it exists
if (file.exists(CACHE_FILE)) {
  backbone_cache <- read_tsv(CACHE_FILE,
                             col_types      = cols(.default = col_character()),
                             show_col_types = FALSE)
  cat(sprintf("  Loaded %d cached backbone results.\n", nrow(backbone_cache)))
} else {
  backbone_cache <- tibble(query_name = character())
  cat("  No cache found — will query all names.\n")
}

# Identify names not yet in cache
new_names <- names_to_resolve %>%
  filter(!query_name %in% backbone_cache$query_name) %>%
  pull(query_name)

if (length(new_names) > 0) {
  cat(sprintf("  Querying GBIF backbone for %d new names", length(new_names)))
  cat(" (this may take a minute)...\n")

  # name_backbone_checklist batches internally (100 at a time)
  query_df <- tibble(name = new_names)
  new_results <- name_backbone_checklist(query_df) %>%
    rename(query_name = verbatim_name) %>%
    mutate(across(everything(), as.character))

  # Append to cache and save
  backbone_cache <- bind_rows(backbone_cache, new_results)
  write_tsv(backbone_cache, CACHE_FILE)
  cat(sprintf("  Cached %d total backbone results.\n", nrow(backbone_cache)))
} else {
  cat("  All names already in cache.\n")
}

# Join backbone results back to our name list
backbone <- names_to_resolve %>%
  left_join(backbone_cache, by = "query_name") %>%
  mutate(
    # Prefer backbone's accepted species name; fall back to query name
    gbif_accepted_name = case_when(
      !is.na(species)  & species  != "" ~ species,
      !is.na(canonicalName)            ~ canonicalName,
      TRUE                             ~ query_name
    ),
    gbif_match_type       = matchType,
    gbif_match_confidence = suppressWarnings(as.integer(confidence)),
    gbif_status           = status,
    gbif_species_key      = speciesKey
  ) %>%
  select(class, query_name, species_in_data,
         gbif_accepted_name, gbif_match_type,
         gbif_match_confidence, gbif_status, gbif_species_key)

cat(sprintf(
  "\n  Match type breakdown:\n"
))
backbone %>% count(gbif_match_type, sort = TRUE) %>% print()

cat("\n  Flagging low-confidence or unmatched names:\n")
backbone %>%
  filter(is.na(gbif_match_type) |
         gbif_match_type == "NONE" |
         gbif_match_confidence < 90) %>%
  select(class, query_name, gbif_match_type, gbif_match_confidence) %>%
  arrange(gbif_match_confidence) %>%
  print(n = 20)

# =========================================================
# SECTION 3: APPLY MANUAL SYNONYM OVERRIDES
# =========================================================
# The synonyms dict in taxonomy.py resolves names that
# appear in GBIF data to IOC-accepted equivalents.  Apply
# them here so the thesaurus is consistent with what
# taxonomy.py produces.

cat("\nSection 3: Applying manual synonym overrides from taxonomy.py...\n")

backbone <- backbone %>%
  mutate(
    manual_override = MANUAL_SYNONYMS[query_name],
    gbif_accepted_name = if_else(
      !is.na(manual_override),
      manual_override,
      gbif_accepted_name
    ),
    override_applied = !is.na(manual_override)
  ) %>%
  select(-manual_override)

n_overrides <- sum(backbone$override_applied, na.rm = TRUE)
cat(sprintf("  Applied %d manual overrides.\n", n_overrides))

# =========================================================
# SECTION 4: CROSS-REFERENCE IOC NAMES (birds)
# =========================================================
# For Aves, cross-check accepted names against the IOC
# World Bird List to flag names not yet in the IOC list
# (possible very recent splits, or non-avian taxa).

cat("\nSection 4: Cross-referencing IOC World Bird List for Aves...\n")

if (file.exists(IOC_FILE)) {
  ioc_xml   <- read_xml(IOC_FILE)
  ioc_names <- xml_find_all(ioc_xml, ".//species/latin_name") %>%
    xml_text() %>%
    trimws()
  cat(sprintf("  IOC list contains %d species.\n", length(ioc_names)))

  backbone <- backbone %>%
    mutate(
      ioc_match = case_when(
        class != "Aves"                          ~ NA_character_,
        gbif_accepted_name %in% ioc_names        ~ "exact",
        query_name         %in% ioc_names        ~ "query_name_only",
        TRUE                                     ~ "not_in_ioc"
      )
    )

  backbone %>%
    filter(class == "Aves") %>%
    count(ioc_match, sort = TRUE) %>%
    print()

  # Warn about Aves names absent from IOC
  not_in_ioc <- backbone %>%
    filter(class == "Aves", ioc_match == "not_in_ioc") %>%
    select(query_name, gbif_accepted_name, gbif_match_type,
           gbif_match_confidence)
  if (nrow(not_in_ioc) > 0) {
    cat(sprintf(
      "\n  %d Aves names not found in IOC list (review recommended):\n",
      nrow(not_in_ioc)
    ))
    print(not_in_ioc, n = 30)
  }
} else {
  message("  IOC file not found at: ", IOC_FILE)
  backbone <- backbone %>% mutate(ioc_match = NA_character_)
}

# =========================================================
# SECTION 5: JOIN CDF GALÁPAGOS CHECKLIST
# =========================================================
# The CDF checklist adds Galápagos-specific status and
# per-island expected ranges — information the global GBIF
# backbone does not provide.
#
# If the file is not yet available, output columns will be
# NA; re-run after adding data/cdf_galapagos_checklist.tsv.

cat("\nSection 5: Joining CDF Galápagos checklist...\n")

if (file.exists(CDF_FILE)) {
  cdf_raw <- read_tsv(CDF_FILE,
                      col_types      = cols(.default = col_character()),
                      show_col_types = FALSE)
  cat(sprintf("  Loaded %d CDF records.\n", nrow(cdf_raw)))

  # Normalise column names to expected values
  cdf <- cdf_raw %>%
    rename(
      cdf_name         = any_of(CDF_COL_NAME),
      galapagos_status = any_of(CDF_COL_STATUS),
      expected_islands = any_of(CDF_COL_ISLANDS)
    ) %>%
    # Add optional columns if absent
    { if (!CDF_COL_COMMON %in% names(.)) mutate(., common_name = NA_character_) else rename(., common_name = any_of(CDF_COL_COMMON)) } %>%
    { if (!CDF_COL_NOTES  %in% names(.)) mutate(., cdf_notes   = NA_character_) else rename(., cdf_notes   = any_of(CDF_COL_NOTES))  } %>%
    select(cdf_name, galapagos_status, expected_islands,
           common_name, cdf_notes)

  # Join on accepted name first, then fall back to query name
  backbone <- backbone %>%
    left_join(cdf, by = c("gbif_accepted_name" = "cdf_name")) %>%
    # For non-matches, try joining on the original query name
    left_join(cdf %>% rename_with(~ paste0(.x, "_q"), -cdf_name),
              by = c("query_name" = "cdf_name")) %>%
    mutate(
      galapagos_status = coalesce(galapagos_status, galapagos_status_q),
      expected_islands = coalesce(expected_islands, expected_islands_q),
      common_name      = coalesce(common_name,      common_name_q),
      cdf_notes        = coalesce(cdf_notes,         cdf_notes_q)
    ) %>%
    select(-ends_with("_q"))

  cat("  CDF status breakdown:\n")
  backbone %>% count(galapagos_status, sort = TRUE) %>% print()

  n_unmatched <- sum(is.na(backbone$galapagos_status))
  if (n_unmatched > 0) {
    cat(sprintf(
      "\n  %d names not matched in CDF checklist:\n", n_unmatched
    ))
    backbone %>%
      filter(is.na(galapagos_status)) %>%
      select(class, gbif_accepted_name, gbif_status) %>%
      arrange(class, gbif_accepted_name) %>%
      print(n = 30)
  }
} else {
  message(
    "  CDF checklist not found at: ", CDF_FILE, "\n",
    "  galapagos_status and expected_islands will be NA.\n",
    "  Download from https://www.darwinfoundation.org/en/datazone/checklist\n",
    "  and save as data/cdf_galapagos_checklist.tsv"
  )
  backbone <- backbone %>%
    mutate(
      galapagos_status = NA_character_,
      expected_islands = NA_character_,
      common_name      = NA_character_,
      cdf_notes        = NA_character_
    )
}

# =========================================================
# SECTION 6: ASSEMBLE AND WRITE THESAURUS
# =========================================================

cat("\nSection 6: Writing thesaurus...\n")

thesaurus <- backbone %>%
  select(
    # Original name as it appears in specimen data
    original_name         = query_name,
    # Canonical name from GBIF backbone + manual overrides
    accepted_name         = gbif_accepted_name,
    # GBIF backbone provenance
    gbif_match_type,
    gbif_match_confidence,
    gbif_status,
    gbif_species_key,
    # IOC cross-reference (Aves only)
    ioc_match,
    # CDF Galápagos-specific data
    galapagos_status,
    expected_islands,
    common_name,
    cdf_notes,
    # Flags
    override_applied,
    # Source class
    class,
    # Original species field from GBIF record (useful for audit)
    species_in_data
  ) %>%
  arrange(class, accepted_name, original_name)

write_tsv(thesaurus, THESAURUS_FILE)

cat(sprintf("Written: %s\n", THESAURUS_FILE))
cat(sprintf("Rows: %d  |  Classes: %s\n",
            nrow(thesaurus),
            paste(sort(unique(thesaurus$class)), collapse = ", ")))

# ── Summary ───────────────────────────────────────────────
cat("\n")
cat(strrep("─", 60), "\n")
cat(sprintf("%-35s %10s\n", "Category", "Count"))
cat(strrep("─", 60), "\n")
cat(sprintf("%-35s %10d\n", "Total names",              nrow(thesaurus)))
cat(sprintf("%-35s %10d\n", "Exact GBIF matches",       sum(thesaurus$gbif_match_type == "EXACT",  na.rm=TRUE)))
cat(sprintf("%-35s %10d\n", "Fuzzy GBIF matches",       sum(thesaurus$gbif_match_type == "FUZZY",  na.rm=TRUE)))
cat(sprintf("%-35s %10d\n", "Unmatched by GBIF",        sum(thesaurus$gbif_match_type == "NONE",   na.rm=TRUE)))
cat(sprintf("%-35s %10d\n", "Manual overrides applied", sum(thesaurus$override_applied, na.rm=TRUE)))
cat(sprintf("%-35s %10d\n", "With CDF status",          sum(!is.na(thesaurus$galapagos_status))))
cat(sprintf("%-35s %10d\n", "Without CDF status (NA)",  sum(is.na(thesaurus$galapagos_status))))
cat(strrep("─", 60), "\n")
cat("Done. Next step: review unmatched names and add to\n")
cat("data/cdf_galapagos_checklist.tsv if missing.\n")
