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
#      synonyms -> accepted names and flag doubtful records
#   3. Cross-referencing the AviList 2025 checklist (already
#      in data/AviList-v2025-11Jun-extended.xlsx) for bird-
#      specific canonical names and English common names
#   4. Joining against the CDF Galapagos species checklists
#      (data/cdf_galapagos_checklists/*.csv -- see note)
#      to add Galapagos-specific status and island ranges
#   5. Incorporating the manual synonym overrides maintained
#      in src/taxonomy.py
#   6. Writing galapagos_thesaurus.tsv for use by
#      species_by_island.R and future downstream scripts
#
# ---- About the CDF checklist files ---------------------
# The Charles Darwin Foundation maintains a Galapagos
# Species Database at:
#   https://www.darwinfoundation.org/en/datazone/checklist
#
# Download per-taxon checklists (birds, reptiles, mammals,
# etc.) and place all CSV files in:
#   data/cdf_galapagos_checklists/
#
# All CDF CSVs share an identical column structure and are
# read together with bind_rows.  To update, simply replace
# or add CSV files in that folder and re-run -- no
# re-concatenation needed.
#
# The CDF CSVs are Latin-1 encoded; this script handles
# that automatically via locale(encoding = "latin1").
#
# If the directory is absent or empty, galapagos_status
# and expected_islands will be NA for all records.
#
# ---- GBIF API caching ----------------------------------
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
library(purrr)
library(rgbif)    # name_backbone_checklist()
library(readxl)   # reading AviList-v2025-*.xlsx

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
REPO_ROOT    <- path.expand("~/galapagos_island_mapper")
AVILIST_FILE <- file.path(REPO_ROOT, "data", "AviList-v2025-11Jun-extended.xlsx")
AVILIST_SHEET <- "AviList v2025 extended"   # sheet name inside the workbook
CDF_DIR      <- file.path(REPO_ROOT, "data", "cdf_galapagos_checklists")
# Note: the IOC World Bird List (data/ioc-names-14.1.xml) has been superseded
# by AviList and is no longer used.

# Vertebrate classes to include (as they appear in GBIF specimen files)
TARGET_CLASSES <- c("Aves", "Mammalia", "Testudines", "Squamata")

# ---- CDF island column -> canonical island name --------
# Maps the per-island presence columns in the CDF CSVs
# (written in Spanish, Latin-1 encoded) to the canonical
# lowercase names used throughout this pipeline.
# Bioregion columns (Elizabeth Bay, Far-northern, etc.)
# are intentionally omitted -- we only track named islands.
ISLAND_COL_MAP <- c(
  "Darwin"             = "darwin",
  "Española"      = "espanola",      # Espanola
  "Fernandina"         = "fernandina",
  "Floreana"           = "floreana",
  "Genovesa"           = "genovesa",
  "Isabela"            = "isabela",
  "Marchena"           = "marchena",
  "Pinta"              = "pinta",
  "Pinzón"        = "pinzon",         # Pinzon
  "San Cristóbal" = "san cristobal",  # San Cristobal
  "Santa Cruz"         = "santa cruz",
  "Santa Fé"      = "santa fe",       # Santa Fe
  "Santiago"           = "santiago",
  "Wolf"               = "wolf"
  # "Unknown Island" intentionally excluded
)

# ---- CDF Origin / Suborigin code mapping ---------------
# Origin:   Na = native, In = introduced, NoData = unknown
# Suborigin: En = endemic, Va = vagrant, Mi = migratory
#             Ac = accidental, Id = indeterminate,
#             Es = established introduced, Ic = intro casual
derive_galapagos_status <- function(origin, suborigin) {
  case_when(
    origin == "In"                                 ~ "introduced",
    origin == "Na" & suborigin == "En"             ~ "endemic",
    origin == "Na" & suborigin == "Mi"             ~ "visitor",
    origin == "Na" & suborigin %in% c("Va", "Ac") ~ "vagrant",
    origin == "Na"                                 ~ "native",
    TRUE                                           ~ "unknown"
  )
}

# ---- Manual synonym overrides from src/taxonomy.py -----
# Bird names that appear in GBIF data but map to a
# different AviList-accepted name.  Keep in sync with the
# synonyms dict in taxonomy.py.
MANUAL_SYNONYMS <- c(
  "Oceanodroma castro"      = "Hydrobates castro",
  "Aphriza virgata"         = "Calidris virgata",
  "Oceanodroma leucorhoa"   = "Hydrobates leucorhous",
  "Phalacrocorax harrisi"   = "Nannopterum harrisi",
  "Puffinus creatopus"      = "Ardenna creatopus",
  "Philomachus pugnax"      = "Calidris pugnax",
  "Anas clypeata"           = "Spatula clypeata",
  "Anas cyanoptera"         = "Spatula cyanoptera",
  "Aratinga erythrogenys"   = "Psittacara erythrogenys",
  "Anas discors"            = "Spatula discors",
  "Puffinus pacificus"      = "Ardenna pacifica",
  "Puffinus griseus"        = "Ardenna grisea",
  "Charadrius wilsonia"     = "Anarhynchus wilsonia",
  "Tryngites subruficollis" = "Calidris subruficollis",
  "Oceanodroma tethys"      = "Hydrobates tethys",
  "Laterallus spilonotus"   = "Laterallus spilonota",
  "Neocrex erythrops"       = "Mustelirallus erythrops",
  "Oceanodroma markhami"    = "Hydrobates markhami",
  "Oceanodroma hornbyi"     = "Hydrobates hornbyi",
  "Oceanodroma microsoma"   = "Hydrobates microsoma"
)

# =========================================================
# SECTION 1: COLLECT UNIQUE SPECIES NAMES
# =========================================================

cat("Section 1: Collecting unique species names from specimen files...\n")

all_names <- map_dfr(SPECIMEN_FILES, function(f) {
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
# Prefer acceptedScientificName (GBIF has already done some
# backbone resolution), falling back to species, then
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
  cat("  No cache found -- will query all names.\n")
}

# Identify names not yet in cache
new_names <- names_to_resolve %>%
  filter(!query_name %in% backbone_cache$query_name) %>%
  pull(query_name)

if (length(new_names) > 0) {
  cat(sprintf("  Querying GBIF backbone for %d new names", length(new_names)))
  cat(" (this may take a minute)...\n")

  # name_backbone_checklist batches internally (100 at a time)
  query_df    <- tibble(name = new_names)
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
    # Prefer backbone's accepted species name; fall back to query
    gbif_accepted_name    = case_when(
      !is.na(species)       & species       != "" ~ species,
      !is.na(canonicalName) & canonicalName != "" ~ canonicalName,
      TRUE                                        ~ query_name
    ),
    gbif_match_type       = matchType,
    gbif_match_confidence = suppressWarnings(as.integer(confidence)),
    gbif_status           = status,
    gbif_species_key      = speciesKey
  ) %>%
  select(class, query_name, species_in_data,
         gbif_accepted_name, gbif_match_type,
         gbif_match_confidence, gbif_status, gbif_species_key)

cat("\n  Match type breakdown:\n")
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
    manual_override    = MANUAL_SYNONYMS[query_name],
    gbif_accepted_name = if_else(
      !is.na(manual_override),
      manual_override,
      gbif_accepted_name
    ),
    override_applied   = !is.na(manual_override)
  ) %>%
  select(-manual_override)

cat(sprintf("  Applied %d manual overrides.\n",
            sum(backbone$override_applied, na.rm = TRUE)))

# =========================================================
# SECTION 4: CROSS-REFERENCE AVILIST 2025 (birds only)
# =========================================================
# AviList 2025 is the successor to both the IOC World Bird
# List and the Clements checklist, intended as a single
# unified world bird checklist.
#
# For Aves, this section:
#   1. Reads species names and English names from AviList
#   2. Joins to backbone to retrieve avilist_english_name
#      for each matched bird species
#   3. Sets avilist_match: "exact" / "query_name_only" /
#      "not_in_avilist" for diagnosing nomenclature gaps
#
# avilist_english_name is later used in Section 5 as a
# fallback for common_name when the CDF has no English name.

cat("\nSection 4: Cross-referencing AviList 2025 for Aves...\n")

if (file.exists(AVILIST_FILE)) {
  avilist <- read_excel(AVILIST_FILE,
                        sheet     = AVILIST_SHEET,
                        col_types = "text") %>%
    # Keep only species-level rows (file also has order/family/genus/subspecies)
    filter(Taxon_rank == "species") %>%
    select(
      avilist_scientific = Scientific_name,
      avilist_english    = English_name_AviList
    ) %>%
    filter(!is.na(avilist_scientific))

  cat(sprintf("  AviList 2025 contains %d species.\n", nrow(avilist)))

  # Join English name via accepted name first, then query name as fallback
  backbone <- backbone %>%
    left_join(
      avilist %>% rename(avilist_eng_accepted = avilist_english),
      by = c("gbif_accepted_name" = "avilist_scientific")
    ) %>%
    left_join(
      avilist %>% rename(avilist_eng_query = avilist_english),
      by = c("query_name" = "avilist_scientific")
    ) %>%
    mutate(
      avilist_english_name = coalesce(avilist_eng_accepted,
                                      avilist_eng_query),
      avilist_match = case_when(
        class != "Aves"                    ~ NA_character_,
        !is.na(avilist_eng_accepted)       ~ "exact",
        !is.na(avilist_eng_query)          ~ "query_name_only",
        TRUE                               ~ "not_in_avilist"
      )
    ) %>%
    select(-avilist_eng_accepted, -avilist_eng_query)

  backbone %>%
    filter(class == "Aves") %>%
    count(avilist_match, sort = TRUE) %>%
    print()

  not_in_avilist <- backbone %>%
    filter(class == "Aves", avilist_match == "not_in_avilist") %>%
    select(query_name, gbif_accepted_name, gbif_match_type,
           gbif_match_confidence)
  if (nrow(not_in_avilist) > 0) {
    cat(sprintf(
      "\n  %d Aves names not found in AviList (review recommended):\n",
      nrow(not_in_avilist)
    ))
    print(not_in_avilist, n = 30)
  }
} else {
  message("  AviList file not found at: ", AVILIST_FILE)
  backbone <- backbone %>%
    mutate(avilist_match        = NA_character_,
           avilist_english_name = NA_character_)
}

# =========================================================
# SECTION 5: JOIN CDF GALAPAGOS CHECKLISTS
# =========================================================
# Reads all CSV files from CDF_DIR (separate per-taxon
# checklists sharing an identical column structure).
#
# Key transformations:
#   - Latin-1 encoding handled at read time
#   - Binomial constructed from Genus + "Specific Epithtet"
#     (note the typo in the CDF column name -- kept as-is)
#   - Only valid species rows kept (Taxon Status == "1");
#     subspecies / informal taxa (status 3) excluded
#   - CDF Class "Reptilia" split into Squamata / Testudines
#     by Order, to match our pipeline's TARGET_CLASSES
#   - Origin + Suborigin codes mapped to galapagos_status
#   - Per-island presence columns ("True" / "") pivoted to
#     a pipe-separated expected_islands string using
#     canonical lowercase names (Spanish -> English)

cat("\nSection 5: Reading and processing CDF Galapagos checklists...\n")

cdf_files <- if (dir.exists(CDF_DIR)) {
  list.files(CDF_DIR, pattern = "\\.csv$", full.names = TRUE,
             ignore.case = TRUE)
} else {
  character(0)
}

if (length(cdf_files) == 0) {
  message(
    "  No CDF checklist CSVs found in: ", CDF_DIR, "\n",
    "  galapagos_status and expected_islands will be NA.\n",
    "  Download from https://www.darwinfoundation.org/en/datazone/checklist\n",
    "  and save CSVs to data/cdf_galapagos_checklists/"
  )
  backbone <- backbone %>%
    mutate(
      galapagos_status = NA_character_,
      expected_islands = NA_character_,
      common_name      = NA_character_,
      cdf_notes        = NA_character_,
      cdf_iucn_status  = NA_character_
    )
} else {
  cat(sprintf("  Found %d CDF checklist file(s).\n", length(cdf_files)))

  # ---- Read all CSVs into one dataframe ----------------
  cdf_raw <- map_dfr(cdf_files, function(f) {
    cat("  Reading:", basename(f), "\n")
    read_csv(f,
             col_types      = cols(.default = col_character()),
             locale         = locale(encoding = "latin1"),
             show_col_types = FALSE)
  })
  cat(sprintf("  Total CDF records loaded: %d\n", nrow(cdf_raw)))

  # ---- Keep only valid species (Taxon Status == "1") ---
  # Status 3 rows are subspecies or informal taxa (e.g.
  # "Alsophis sp.") that won't match GBIF specimen names.
  cdf_raw <- cdf_raw %>%
    filter(coalesce(`Taxon Status`, "") == "1")
  cat(sprintf("  After keeping valid species (Taxon Status=1): %d records\n",
              nrow(cdf_raw)))

  # ---- Construct binomial species name -----------------
  # The CDF stores genus and epithet in separate columns.
  # Note: the column is spelled "Specific Epithtet" (sic)
  # in all CDF checklist files.
  cdf_raw <- cdf_raw %>%
    mutate(
      cdf_name = str_trim(paste(
        str_trim(coalesce(Genus, "")),
        str_trim(coalesce(`Specific Epithtet`, ""))
      )),
      cdf_name = na_if(str_replace(cdf_name, "\\s+", " "), ""),
      cdf_name = na_if(cdf_name, "NA NA")
    ) %>%
    filter(!is.na(cdf_name))

  # ---- Map CDF Class -> pipeline TARGET_CLASSES --------
  # The CDF uses "Reptilia" for the whole class; split by
  # Order to match how our pipeline distinguishes Squamata
  # from Testudines.
  cdf_raw <- cdf_raw %>%
    mutate(
      pipeline_class = case_when(
        Class == "Aves"     ~ "Aves",
        Class == "Mammalia" ~ "Mammalia",
        Class == "Reptilia" & Order == "Testudines" ~ "Testudines",
        Class == "Reptilia" & Order == "Squamata"   ~ "Squamata",
        TRUE ~ NA_character_   # fish, amphibians etc. -- excluded
      )
    ) %>%
    filter(!is.na(pipeline_class))

  cat(sprintf("  After filtering to target classes: %d records\n",
              nrow(cdf_raw)))
  cat("  Class breakdown:\n")
  cdf_raw %>% count(pipeline_class, sort = TRUE) %>% print()

  # ---- Derive galapagos_status -------------------------
  cdf_raw <- cdf_raw %>%
    mutate(galapagos_status = derive_galapagos_status(Origin, Suborigin))

  cat("  Status breakdown:\n")
  cdf_raw %>% count(galapagos_status, sort = TRUE) %>% print()

  # ---- Pivot island presence columns -------------------
  # The CDF encodes per-island presence as "True" or "".
  # Only include island columns that are actually present
  # in the loaded data (tolerates partial file sets).
  present_island_cols <- intersect(names(ISLAND_COL_MAP), names(cdf_raw))
  cat(sprintf("  Island columns found: %d of %d expected\n",
              length(present_island_cols), length(ISLAND_COL_MAP)))

  cdf_islands <- cdf_raw %>%
    select(cdf_name, all_of(present_island_cols)) %>%
    pivot_longer(
      cols      = all_of(present_island_cols),
      names_to  = "cdf_island_col",
      values_to = "present"
    ) %>%
    filter(present == "True") %>%
    mutate(island = ISLAND_COL_MAP[cdf_island_col]) %>%
    group_by(cdf_name) %>%
    summarise(
      expected_islands = paste(sort(island), collapse = "|"),
      .groups = "drop"
    )

  # ---- Assemble final CDF lookup table -----------------
  # Deduplicate on cdf_name: for species appearing in
  # multiple files (shouldn't happen but is defensive),
  # keep the first row encountered.
  cdf <- cdf_raw %>%
    select(
      cdf_name,
      pipeline_class,
      galapagos_status,
      cdf_iucn_status = `IUCN Status`,
      common_name     = `English Common Name`,
      cdf_notes       = `Distribution Comments`
    ) %>%
    distinct(cdf_name, .keep_all = TRUE) %>%
    left_join(cdf_islands, by = "cdf_name")

  cat(sprintf("  CDF lookup table: %d unique species entries.\n\n",
              nrow(cdf)))

  # ---- Join to backbone --------------------------------
  # Try gbif_accepted_name first; fall back to query_name
  # for cases where GBIF resolved the name differently than
  # the CDF uses.
  backbone <- backbone %>%
    left_join(
      cdf %>% select(cdf_name, galapagos_status, expected_islands,
                     common_name, cdf_notes, cdf_iucn_status),
      by = c("gbif_accepted_name" = "cdf_name")
    ) %>%
    left_join(
      cdf %>% select(cdf_name,
                     galapagos_status_q = galapagos_status,
                     expected_islands_q = expected_islands,
                     common_name_q      = common_name,
                     cdf_notes_q        = cdf_notes,
                     cdf_iucn_status_q  = cdf_iucn_status),
      by = c("query_name" = "cdf_name")
    ) %>%
    mutate(
      galapagos_status = coalesce(galapagos_status, galapagos_status_q),
      expected_islands = coalesce(expected_islands, expected_islands_q),
      common_name      = coalesce(common_name,      common_name_q),
      cdf_notes        = coalesce(cdf_notes,         cdf_notes_q),
      cdf_iucn_status  = coalesce(cdf_iucn_status,  cdf_iucn_status_q)
    ) %>%
    select(-ends_with("_q"))

  cat("  CDF status breakdown after join:\n")
  backbone %>% count(galapagos_status, sort = TRUE) %>% print()

  n_unmatched <- sum(is.na(backbone$galapagos_status))
  if (n_unmatched > 0) {
    cat(sprintf("\n  %d names not matched in CDF checklist:\n", n_unmatched))
    backbone %>%
      filter(is.na(galapagos_status)) %>%
      select(class, gbif_accepted_name, gbif_status) %>%
      arrange(class, gbif_accepted_name) %>%
      print(n = 30)
  }
}

# ---- Fill common_name gap with AviList English name -----
# For Aves records where the CDF join left common_name blank,
# use the AviList English name as a fallback.  Non-bird
# classes are unaffected (avilist_english_name is NA there).
if ("avilist_english_name" %in% names(backbone)) {
  n_filled <- sum(is.na(backbone$common_name) &
                  !is.na(backbone$avilist_english_name), na.rm = TRUE)
  backbone <- backbone %>%
    mutate(common_name = coalesce(common_name, avilist_english_name))
  if (n_filled > 0)
    cat(sprintf("\n  Filled %d missing common_name values from AviList.\n",
                n_filled))
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
    # AviList 2025 cross-reference (Aves only)
    avilist_match,
    avilist_english_name,
    # CDF Galapagos-specific data
    galapagos_status,
    cdf_iucn_status,
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

dir.create(path.expand(OUTPUT_DIR), recursive = TRUE, showWarnings = FALSE)
write_tsv(thesaurus, THESAURUS_FILE)

cat(sprintf("Written: %s\n", THESAURUS_FILE))
cat(sprintf("Rows: %d  |  Classes: %s\n",
            nrow(thesaurus),
            paste(sort(unique(thesaurus$class)), collapse = ", ")))

# ---- Summary --------------------------------------------
cat("\n")
cat(strrep("-", 60), "\n")
cat(sprintf("%-35s %10s\n", "Category", "Count"))
cat(strrep("-", 60), "\n")
cat(sprintf("%-35s %10d\n", "Total names",
            nrow(thesaurus)))
cat(sprintf("%-35s %10d\n", "Exact GBIF matches",
            sum(thesaurus$gbif_match_type == "EXACT",  na.rm = TRUE)))
cat(sprintf("%-35s %10d\n", "Fuzzy GBIF matches",
            sum(thesaurus$gbif_match_type == "FUZZY",  na.rm = TRUE)))
cat(sprintf("%-35s %10d\n", "Unmatched by GBIF",
            sum(thesaurus$gbif_match_type == "NONE",   na.rm = TRUE)))
cat(sprintf("%-35s %10d\n", "Manual overrides applied",
            sum(thesaurus$override_applied, na.rm = TRUE)))
cat(sprintf("%-35s %10d\n", "With CDF status",
            sum(!is.na(thesaurus$galapagos_status))))
cat(sprintf("%-35s %10d\n", "Without CDF status (NA)",
            sum(is.na(thesaurus$galapagos_status))))
cat(strrep("-", 60), "\n")
cat("Done. Review unmatched names above -- names absent from\n")
cat("the CDF checklist may need manual overrides or indicate\n")
cat("visitor/migratory species not in the CDF database.\n")
