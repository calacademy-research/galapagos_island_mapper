# Galápagos Island Mapper — User Guide

**Project:** `galapagos_island_mapper`  
**Contact:** Jack Dumbacher — jdumbacher@calacademy.org  
**Last updated:** 2026-09-10

---

## What this project does

This pipeline takes GBIF museum specimen occurrence records for Ecuador and produces a clean, curated database of specimens assignd to specific Galápagos islands. It resolves each record to an island using GPS coordinates and/or locality text, filters out mainland Ecuador contamination, applies Galápagos-specific taxonomic corrections, and outputs species × island summary tables ready for analysis.

The primary outputs are TSV files listing which species have been collected on which islands, with record counts and most recent collection year per species per island.

---

## Prerequisites

### Software

- **Python 3.9+** with the packages in `requirements.txt`:
  ```bash
  pip install -r requirements.txt
  ```
- **R 4.2+** with the following packages:
  ```r
  install.packages(c("dplyr", "tidyr", "readr", "stringr", "purrr",
                     "data.table", "rgbif", "readxl"))
  ```

### Accounts and credentials

- **GBIF account** — required to submit download requests. Create one free at https://www.gbif.org/user/profile. Add your credentials to `~/.Renviron` (never in the script itself):
  ```
  GBIF_USER=your_gbif_username
  GBIF_PWD=your_gbif_password
  GBIF_EMAIL=your_email@example.com
  ```

### Reference data files

The following files must be present in the `data/` directory before running the thesaurus builder. They are not included in the repository due to size or license constraints.

| File | Where to get it |
|---|---|
| `data/AviList-v2025-11Jun-extended.xlsx` | Download from [AviList](https://avilist.org) — the 2025 unified world bird checklist |
| `data/cdf_galapagos_checklists/*.csv` | Download per-taxon checklists from the [CDF Datazone](https://www.darwinfoundation.org/en/datazone/checklist); place all CSV files in this directory |

The `ioc-names-14.1.xml` file already in `data/` is retained for reference but is no longer used by any script (superseded by AviList 2025).

---

## Directory structure

```
galapagos_island_mapper/
├── data/
│   ├── AviList-v2025-11Jun-extended.xlsx   # Bird checklist (download separately)
│   ├── cdf_galapagos_checklists/           # CDF CSVs (download separately)
│   ├── galapagos.geojson                   # Island polygons (included)
│   └── ioc-names-14.1.xml                  # Legacy IOC list (retained, unused)
├── src/
│   ├── analyze.py      # Island assignment engine
│   ├── islands.py      # Island definitions and name aliases
│   ├── latlon.py       # Coordinate-based island resolver
│   ├── name.py         # Text-field-based island resolver
│   ├── process.py      # Pipeline orchestrator
│   └── taxonomy.py     # Taxonomic name mapper
├── r/
│   ├── gbif_ecuador_download.R      # Step 1: download, filter, assign
│   ├── build_galapagos_thesaurus.R  # Step 2: build taxonomic thesaurus
│   ├── refine_taxonomy.R            # Step 3: apply thesaurus to specimens
│   ├── species_by_island.R          # Step 4: build output tables
│   ├── gbif_data_ingester.R         # Optional: multi-pull concatenation
│   ├── gbif_galapagos_gadm_download.R  # Optional: GADM ground-truth pull
│   └── load_galapagos_data.R        # Helper: load outputs into R
├── analyze.sh          # Shell wrapper for analyze.py
├── config.ini          # Path configuration
└── LAB_NOTEBOOK.md     # Detailed technical notes
```

---

## Data storage

The pipeline reads from and writes to a directory outside the repository. By default this is `~/Dropbox/Galapagos_data/`, with two subdirectories:

- `~/Dropbox/Galapagos_data/input/` — raw GBIF download files (large; not in git)
- `~/Dropbox/Galapagos_data/output/` — all processed output TSV files

You can change these paths by editing the `OUTPUT_DIR` and `INPUT_DIR` variables at the top of each R script.

---

## Running the pipeline

The pipeline has four sequential steps. Run them in order. Each step depends on the outputs of the previous one.

### Step 1 — Download and filter GBIF data (`r/gbif_ecuador_download.R`)

This script downloads all Ecuador specimen records from GBIF, runs the island-assignment engine on them, and filters the results to confirmed Galápagos specimens.

**First-time setup:** Set `REDOWNLOAD <- TRUE` near the top of the script. This submits a download job to GBIF (requires credentials in `~/.Renviron`). For ~2.3 million Ecuador records, GBIF typically takes 30 minutes to a few hours to compile the archive. The script will wait automatically and then download and unzip the file.

**Subsequent runs:** Leave `REDOWNLOAD <- FALSE` (the default). The script will use the existing input file and skip the download.

```r
source("r/gbif_ecuador_download.R")
```

**What it produces** (all in `~/Dropbox/Galapagos_data/output/`):

| File | Contents |
|---|---|
| `galapagos_specimens.tsv` | Island-resolved, contamination-filtered specimen records |
| `galapagos_unresolved.tsv` | Confirmed Galápagos records that could not be assigned to a specific island |
| `galapagos_all.tsv` | Both of the above combined (`best = NA` for unresolved records) |

**What the filter does:** The script applies a three-step contamination filter to separate genuine Galápagos specimens from the ~32,000 mainland Ecuador records that the island-assignment engine incorrectly resolves to Galápagos island names (e.g., "Morona-Santiago" matching "Santiago" Island). A record is kept only if it passes at least one of five provenance conditions: GPS coordinates in the archipelago, GBIF's own GADM geotag for Galápagos, stateProvince = Galápagos, any locality field mentioning "Galápagos", or an old English island name (Albemarle, Narborough, Indefatigable, etc.) in the locality text.

**Sanity check:** At the end, the script prints a spot-check of CAS Aves records. A healthy run produces ~9,860 bird records across 23 islands. If this number is very different, something may be wrong with the filter.

---

### Step 1b — Island assignment (runs automatically)

You do not need to run this separately. `gbif_ecuador_download.R` calls `analyze.sh`, which runs the Python island-assignment engine (`src/analyze.py`) on the input TSV. It reads every occurrence record and assigns it to a Galápagos island using two resolvers:

- **LatLonResolver** — matches GPS coordinates against island polygons (OSM geometry in `data/galapagos.geojson`)
- **NameResolver** — searches locality text fields for island names, historical English names, and named places (Academy Bay → Santa Cruz, Tagus Cove → Isabela, etc.)

The result is a `results.tsv` file with one row per input record and four columns: `gbifID`, `latlon` (island from coordinates), `name` (island from text), `best` (final combined assignment), and `species`.

If you need to re-run the island assignment without re-downloading from GBIF (e.g., after modifying `islands.py`), run:

```bash
bash analyze.sh ~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv
```

then re-source `gbif_ecuador_download.R` with `REDOWNLOAD <- FALSE`.

---

### Step 2 — Build the taxonomic thesaurus (`r/build_galapagos_thesaurus.R`)

This script builds a lookup table that maps every species name found in the pipeline outputs to a GBIF-accepted canonical name, cross-referenced with AviList 2025 (for birds) and the CDF Galápagos checklist (for all vertebrates). It also records which islands each species is expected to occur on, according to the CDF.

```r
source("r/build_galapagos_thesaurus.R")
```

**What it produces:**

- `galapagos_thesaurus.tsv` — one row per unique species name, with accepted name, match confidence, Galápagos status (endemic / native / visitor / vagrant / introduced), expected islands, and common name

**GBIF backbone queries are cached** to `gbif_backbone_cache.tsv`. On first run this makes ~1,500 API calls; subsequent runs skip names already in the cache and complete in seconds. Delete the cache file to force a full re-query (e.g., after a major GBIF backbone update).

**Re-run when:** new species names appear in the specimen files (after a fresh GBIF download), or when you update the CDF checklist files or AviList.

---

### Step 3 — Apply taxonomic refinement (`r/refine_taxonomy.R`)

This script reads the specimen files from Step 1 and the thesaurus from Step 2, and adds two new columns to each specimen record:

- `accepted_name` — the Galápagos-appropriate canonical name, after synonym resolution and island-informed corrections
- `taxonomy_note` — a controlled-vocabulary flag explaining what (if anything) was changed

```r
source("r/refine_taxonomy.R")
```

**What it produces:**

- `galapagos_specimens_refined.tsv`
- `galapagos_specimens_multipull_refined.tsv` (if the multi-pull file exists)
- `galapagos_gadm_specimens_refined.tsv` (if the GADM file exists)

**Key corrections applied:**

- Synonym resolution: e.g., *Nesomimus parvulus* → *Mimus parvulus*
- Island-informed reassignment: e.g., a specimen labeled *Mimus trifasciatus* from Española is reassigned to *Mimus macdonaldi*, the only mockingbird the CDF records for that island
- Genus-level upgrades: e.g., a record identified only as "Mimus" from Española is upgraded to *Mimus macdonaldi* when it is the only Mimus species recorded there

**No records are dropped.** When a correction cannot be made unambiguously (e.g., multiple congeneric species are expected on the island), the original name is retained and the `taxonomy_note` explains why.

**Re-run when:** Step 1 or Step 2 produces updated output files. The refined files are not automatically regenerated — if you update the base specimen files but forget to re-run this step, `species_by_island.R` will silently use stale data.

---

### Step 4 — Build species × island tables (`r/species_by_island.R`)

This script reads the refined specimen files and produces the final summary tables.

```r
source("r/species_by_island.R")
```

**What it produces** (in `~/Dropbox/Galapagos_data/output/species_by_island/`):

| File | Contents |
|---|---|
| `aves_record_counts.tsv` | Birds: number of specimens per species per island |
| `aves_last_year.tsv` | Birds: most recent collection year per species per island |
| `mammalia_record_counts.tsv` | Same for mammals |
| `mammalia_last_year.tsv` | |
| `testudines_record_counts.tsv` | Same for tortoises |
| `testudines_last_year.tsv` | |
| `squamata_record_counts.tsv` | Same for lizards and snakes |
| `squamata_last_year.tsv` | |

In all tables, rows are species (sorted by GBIF taxonomic order, then family, then species name), and columns are islands. The rightmost column, `archipelago`, counts records confirmed as Galápagos but not resolved to a specific island. The first three columns are `species_name`, `taxon_order`, and `family`.

**Configuration options** at the top of the script:

- `USE_REFINED <- TRUE` (default) — uses `accepted_name` from the refined files as row labels. Set to `FALSE` to use raw GBIF species names and reproduce pre-thesaurus output.
- `REQUIRE_SPECIES <- TRUE` (default) — drops records with no species-level identification. Set to `FALSE` to include genus-only records.
- `TARGET_CLASSES` — the four vertebrate classes summarized. Edit to add or remove classes.

---

## Full pipeline summary

```bash
# Terminal: run island assignment (only needed after downloading new GBIF data
# or modifying analyze.py / islands.py)
bash analyze.sh ~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv
```

```r
# R: run all four steps in order
source("r/gbif_ecuador_download.R")         # Step 1 (set REDOWNLOAD=TRUE first time)
source("r/build_galapagos_thesaurus.R")     # Step 2
source("r/refine_taxonomy.R")               # Step 3
source("r/species_by_island.R")             # Step 4
```

For everyday use (re-running the R pipeline on existing GBIF data with no changes to Python code), Steps 1b and 2 can usually be skipped:

```r
source("r/gbif_ecuador_download.R")    # REDOWNLOAD <- FALSE (default)
source("r/refine_taxonomy.R")          # re-run if Step 1 produced new output
source("r/species_by_island.R")
```

---

## Optional pipelines

### Pipeline B — Multi-pull concatenation (`r/gbif_data_ingester.R`)

Concatenates multiple GBIF downloads taken at different times, capturing records that have since been removed or updated in GBIF. Applies the same filter logic as Pipeline A. Produces `galapagos_specimens_multipull.tsv`. Useful for longitudinal studies or when you want to maximize record recovery.

### Pipeline C — GADM ground-truth pull (`r/gbif_galapagos_gadm_download.R`)

Downloads all GBIF records that GBIF itself has already tagged with the Galápagos GADM polygon (`gadmGid = ECU.9_1`). Because GBIF applies the geographic filter server-side, no post-hoc contamination filtering is needed. Used primarily as a ground-truth comparison to evaluate how many records Pipelines A/B recover correctly. Not intended as the primary working dataset.

### Loading all outputs at once (`r/load_galapagos_data.R`)

A convenience helper that loads all pipeline output files into your R environment in one call:

```r
source("r/load_galapagos_data.R")
```

---

## Interpreting the outputs

### Which file should I use for analysis?

| Analysis goal | Recommended file |
|---|---|
| Species × island occurrence (the main use case) | `species_by_island/*.tsv` |
| Record-level analysis with best taxonomy | `galapagos_specimens_refined.tsv` |
| Record-level analysis, raw GBIF names | `galapagos_specimens.tsv` |
| All confirmed Galápagos records (incl. unresolved) | `galapagos_all.tsv` |
| Total species richness regardless of island | `galapagos_all.tsv` |

### Understanding `taxonomy_note`

Every record in the refined files has a `taxonomy_note` column explaining what (if anything) was changed from the raw GBIF name:

| Value | Meaning |
|---|---|
| `accepted` | Name was already canonical and island is within expected CDF range |
| `synonym_resolved` | Old synonym resolved to current accepted name via GBIF backbone |
| `manual_override` | Name corrected via a hardcoded override (for Galápagos-specific splits not yet in GBIF) |
| `genus_to_species_by_island` | Genus-only record upgraded to species level (only one species of that genus expected on the island) |
| `species_reassigned_by_island` | Species name not expected on this island; reassigned to the one congeneric species the CDF does expect here |
| `genus_ambiguous` | Genus-only record; multiple congeneric species expected on island — could not pick one |
| `island_mismatch_ambiguous` | Species not expected on island; multiple congeneric alternatives — could not pick one |
| `island_mismatch_unresolved` | Species not expected on island; no congeneric species in CDF for this island |
| `not_in_thesaurus` | Name not found in thesaurus (uncommon visitor or very recent description) |
| `class_not_targeted` | Class outside the four target vertebrate classes; no refinement attempted |
| `no_name` | Record has no usable species or genus identification |

Records with `island_mismatch_*` or `genus_ambiguous` notes retain their original name and are good candidates for manual review.

---

## Troubleshooting

**GBIF download fails with authentication error**  
Check that `GBIF_USER`, `GBIF_PWD`, and `GBIF_EMAIL` are set in `~/.Renviron` and that you have restarted R since adding them. Verify credentials at https://www.gbif.org/user/profile.

**Download key saved but session ended before file was retrieved**  
Read the key from `~/Dropbox/Galapagos_data/input/last_gbif_download_key.txt` and retrieve the file:
```r
key <- readLines("~/Dropbox/Galapagos_data/input/last_gbif_download_key.txt")
zip_path <- occ_download_get(key, path = "~/Dropbox/Galapagos_data/input/")
unzip(zip_path, files = "occurrence.txt",
      exdir = "~/Dropbox/Galapagos_data/input/")
file.rename("~/Dropbox/Galapagos_data/input/occurrence.txt",
            "~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv")
```
GBIF keeps completed downloads available for 6 months.

**RStudio caching error ("invalid first argument / zero-length variable name")**  
GBIF TSV files sometimes have a trailing tab in the header row that creates an empty-named column. The scripts handle this with `select(-any_of(""))` before joins. If you see this error in a different script, add the same line before any `inner_join` or `left_join` call.

**`species_by_island.R` shows a warning about `bad_best` records**  
This means some records in the refined specimens file have a missing or invalid `best` value. The most likely cause is that `galapagos_specimens_refined.tsv` is stale — it was produced by an earlier run of `refine_taxonomy.R` before the base `galapagos_specimens.tsv` was updated. Re-run Steps 3 and 4. To verify whether the base file is clean:
```r
system("wc -l ~/Dropbox/Galapagos_data/output/galapagos_specimens.tsv")
# Should match the "After province/locality filter" count from Step 1 output (+ 1 for header)
```

**The thesaurus build is taking a very long time**  
The first run queries the GBIF backbone for ~1,500 names, which takes a few minutes. Subsequent runs use the cache and complete in seconds. If it is slow on every run, check that `gbif_backbone_cache.tsv` is being written to the output directory and is not being deleted between runs.

**CDF checklist not loading**  
Confirm that at least one `*.csv` file exists in `data/cdf_galapagos_checklists/`. The script degrades gracefully if the directory is absent (all records get `galapagos_status = NA`), but CDF-based island corrections will not be applied.

---

## For developers and maintainers

See `LAB_NOTEBOOK.md` for full technical documentation, including detailed filter logic, the island-assignment algorithm, taxonomy note controlled vocabulary, and the complete development history.

---

*For questions, contact Jack Dumbacher (jdumbacher@calacademy.org).*
