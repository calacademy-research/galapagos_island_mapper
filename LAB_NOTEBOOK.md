# Galápagos Island Mapper — Lab Notebook

**Project:** `galapagos_island_mapper`  
**Maintainer:** Jack Dumbacher — jdumbacher@calacademy.org  
**Last updated:** 2026-05-15  

---

## Project Overview

This project builds a curated database of museum specimen records from the Galápagos Islands by downloading GBIF occurrence data, assigning each record to a specific island using a custom Python resolver (`analyze.py`), and applying a multi-condition contamination filter in R to remove mainland Ecuador records that slip through.

The primary deliverable is a set of TSV files in `~/Dropbox/Galapagos_data/output/` containing island-resolved, contamination-filtered specimen records ready for species × island analysis.

---

## Repository Structure

```
galapagos_island_mapper/
├── src/
│   ├── analyze.py          # Main island-assignment engine (called by analyze.sh)
│   ├── base.py             # Resolver base classes and Resolution datatype
│   ├── islands.py          # Island definitions, OSM IDs, and name aliases
│   ├── latlon.py           # LatLonResolver — coordinate-based island assignment
│   ├── name.py             # NameResolver — text-field-based island assignment
│   ├── process.py          # Pipeline orchestrator; RESOLVERS list; Chooser logic
│   └── taxonomy.py         # Taxonomic name mapper (IOC dictionary)
├── r/
│   ├── gbif_ecuador_download.R      # Pipeline A — single current Ecuador GBIF pull
│   ├── gbif_data_ingester.R         # Pipeline B — multi-pull concatenation
│   ├── gbif_galapagos_gadm_download.R  # Pipeline C — GADM ECU.9_1 ground-truth pull
│   ├── load_galapagos_data.R        # Helper: loads all pipeline outputs into R
│   ├── species_by_island.R          # Species × island summary tables
│   └── [other diagnostic scripts]
├── analyze.sh              # Shell wrapper: runs analyze.py on a TSV input
└── LAB_NOTEBOOK.md         # This file
```

---

## Pipeline Architecture

### Three parallel GBIF download pipelines

**Pipeline A — `gbif_ecuador_download.R`**  
Single current GBIF download filtered to `country=EC` (Ecuador) and `basisOfRecord` = physical specimens. Downloads a DwC-A archive, imports it, and runs the full analyze → filter → output workflow. Output: `galapagos_specimens.tsv`, `galapagos_unresolved.tsv`.

**Pipeline B — `gbif_data_ingester.R`**  
Concatenates multiple GBIF pulls across time (capturing records that have since been removed or updated). Deduplicates by `gbifID`. Mirrors Pipeline A's filter logic exactly. Output: `galapagos_specimens_multipull.tsv`, `galapagos_unresolved_multipull.tsv`.

**Pipeline C — `gbif_galapagos_gadm_download.R`**  
Downloads all GBIF records (any `basisOfRecord`) that GBIF has already tagged with `gadmGid = ECU.9_1` (Galápagos province polygon from gadm.org). Because GBIF applies this filter server-side, no post-hoc contamination filtering is needed. Used as a **ground-truth comparison** to evaluate how many records Pipelines A/B recover or miss. Download key: `0035498-260409193756587`. Yields ~600K total records, ~100K physical specimens. Output: `galapagos_gadm_all.tsv`, `galapagos_gadm_specimens.tsv`.

### Workflow for Pipelines A and B

```
1. GBIF download (REDOWNLOAD=TRUE once)
         ↓
2. Save raw TSV to ~/Dropbox/Galapagos_data/input/
         ↓
3. bash analyze.sh <input.tsv>
   → results.tsv   (columns: gbifID, latlon, name, best, species)
         ↓
4. R script: merge results.tsv → filter → write output TSVs
```

### Output files (all in `~/Dropbox/Galapagos_data/output/`)

| File | Pipeline | Contents |
|---|---|---|
| `galapagos_specimens.tsv` | A | Island-resolved, filtered specimens |
| `galapagos_unresolved.tsv` | A | Confirmed Galápagos, no specific island |
| `galapagos_specimens_multipull.tsv` | B | Same as A, multi-pull |
| `galapagos_unresolved_multipull.tsv` | B | Same as A, multi-pull |
| `galapagos_gadm_all.tsv` | C | All GBIF types, GADM-filtered |
| `galapagos_gadm_specimens.tsv` | C | Specimens only, GADM-filtered |
| `output/species_by_island/*.tsv` | post | Species × island count and last-year tables |

---

## analyze.py — Island Assignment Engine

`analyze.py` processes every record in the input TSV and writes `results.tsv` with four key columns:

- **`latlon`** — island assigned by `LatLonResolver` (coordinate-based polygon matching)
- **`name`** — island assigned by `NameResolver` (text-field-based name matching)
- **`best`** — final combined island assignment (from `Chooser` in `process.py`); `"-"` if unresolved
- **`species`** — most specific taxon name found in the record

### NameResolver — field search order and scoring

The name resolver searches GBIF text fields in order, returning as soon as the first field yields a match. The `adj` value is added to every score from that column; confidence thresholds are HIGH (>7), MODERATE (3–7), LOW (<3).

```python
name_columns = {
    "island":            +1,   # Dedicated island field — highest weight
    "locality":           0,   # Primary locality
    "verbatimLocality":   0,   # Original locality text
    "islandGroup":        0,   # Island group (sometimes holds specific island name)
    "county":             0,   # Last among adj=0: older records sometimes list
                               # the provincial capital (San Cristóbal) as county
                               # even for specimens from other islands
    "locationRemarks":   -1,   # Location-specific free-text notes
    "stateProvince":     -1,   # Coarse admin field; often holds specific island
                               # name (e.g. "Isla Isabela (Albemarle)") but also
                               # province-level labels
    "occurrenceRemarks": -1,   # Most general free-text; consulted last
}
```

**Key design decisions:**
- `county` was moved from second position to last among adj=0 fields (2026-05-15) because older museum records sometimes recorded the provincial capital (San Cristóbal, the seat of Galápagos province) as county even for specimens collected on other islands.
- `stateProvince`, `islandGroup`, `locationRemarks`, and `occurrenceRemarks` were added in April–May 2026 to recover records where island information exists only in those fields.
- The resolver returns after the **first** column that yields a match, so ordering is critical — more reliable fields are always consulted before less reliable ones.

### Named place lookup (`place_islands` dict)

In addition to island name matching, the resolver maintains a dictionary of ~50 named locations (bays, coves, towns, landmarks) that unambiguously identify a single island — e.g. `"academy bay" → "santa cruz"`, `"tagus cove" → "isabela"`, `"darwin bay" → "genovesa"`. These score HIGH confidence (8 points).

### Island aliases (`islands.py`)

The `islands.py` file defines the canonical island names, OSM polygon IDs, and aliases including historical English names (Albemarle, Narborough, Indefatigable, Chatham, etc.) and common misspellings. The Levenshtein distance threshold for fuzzy matching is ≤ 1.

Notable alias added 2026-05-15: `"sta cruz"` for Santa Cruz — appears in German natural history collection records (e.g. stateProvince = "Galapagos Eil., Sta Cruz") where the distance of 2 from "santa cruz" exceeds the fuzzy threshold.

---

## R Filter Logic (Pipelines A and B)

After `analyze.py` assigns `best`, the R scripts apply a three-step filter:

### Step 1 — Keep only island-resolved records
```r
filter(best != "-", !is.na(best), best != "")
```
Records where analyze.py could not assign any island are separated into the `galapagos_unresolved` output.

### Step 2 — Longitude cutoff
```r
MAINLAND_LON_CUTOFF <- -88   # Galápagos easternmost point ~-89.2°
filter(is.na(lon_num) | lon_num <= MAINLAND_LON_CUTOFF)
```
Drops records whose decimal longitude places them clearly on the mainland. Records with no coordinates pass (they may be legitimate pre-GPS specimens resolved by name).

### Step 3 — Galápagos provenance filter (conditions A–E)
A record is kept if **any** of these is TRUE:

| Condition | Field(s) checked | Rationale |
|---|---|---|
| **A** `latlon_confirmed` | `latlon` column from results.tsv | LatLon resolver placed specimen in Galápagos — highly reliable |
| **B** `gadm_galapagos` | `level1Gid == "ECU.9_1"` | GBIF-assigned GADM polygon for Galápagos province |
| **C** `province_galapagos` | `stateProvince` | Matches regex `al[aá]?pag` (all Galápagos spellings/accents) |
| **D** `locality_galapagos` | `locality`, `verbatimLocality`, `island`, `islandGroup`, `county`, `occurrenceRemarks`, `locationRemarks` | Any of these fields mentions "galápago" |
| **E** `english_island` | `locality`, `verbatimLocality`, `island` | Contains an old English Galápagos island name (Albemarle, Narborough, Indefatigable, Chatham, James, Charles, Tower, Bindloe, Abingdon, Jervis, Barrington, Culpepper, Wenman, North Seymour, South Seymour, Duncan) |

**Note on former condition C (removed 2026-04-19):** An earlier version kept records where `stateProvince` was NA. This was allowing large numbers of mainland records through because the name resolver matched continental place names (Santiago province, Santa Cruz canton in Guayas, Morona-Santiago) to Galápagos island names. Removing it eliminated ~176K false positives.

### Unresolved record filter (tightened 2026-05-12)

Records written to `galapagos_unresolved.tsv` (best="-") are filtered to `stateProvince = Galápagos` **plus** at least one of conditions B, D, or E. Records with stateProvince=Galápagos as their **only** anchor are excluded from the unresolved file, since a mislabeled stateProvince on a mainland record would otherwise introduce mainland species into the "archipelago" column of `species_by_island.R` output.

---

## Filter Performance (Pipeline A, run 2026-05-15)

| Stage | Records | Change |
|---|---|---|
| Total `ecuador_data_merged` | 2,272,463 | — |
| Resolved to island by analyze.py | 228,706 | ~10% of total |
| After mainland longitude filter (Step 2) | 215,646 | −13,060 |
| After provenance filter (Step 3) | **183,322** | −32,324 |

**Condition breakdown (not mutually exclusive):**

| Condition | Records |
|---|---|
| (A) lat/lon resolved | 130,335 |
| (B) GADM = ECU.9_1 | 101,497 |
| (C) province = Galápagos | 145,505 |
| (D) locality/county/remarks = Galápagos | 54,275 |
| (E) old English island names | 33,486 |
| **TOTAL kept** | **183,322** |
| TOTAL dropped (contamination) | 32,324 |

**stateProvince breakdown of retained records:**

| Group | Records |
|---|---|
| Galápagos province | 145,505 |
| Other province (latlon/locality confirmed) | 20,881 |
| GADM = ECU.9_1 (no province text) | 16,936 |

**Top contamination sources (dropped records):**

The dominant contamination pathway is the name resolver matching "Santiago" in the mainland province name "Morona-Santiago" to Santiago Island. Three spelling variants combined account for ~14,195 records (~44% of all contamination dropped). All are correctly caught by Step 3 (Morona-Santiago fails every condition A–E).

| stateProvince | Dropped |
|---|---|
| NA | 8,757 |
| Morona-Santiago | 7,331 |
| Morona Santiago | 4,546 |
| Morona - Santiago | 2,318 |
| Pichincha | 1,143 |
| El Oro | 858 |
| Esmeraldas | 732 |
| (+ other mainland provinces) | ... |

**CAS Aves spot-check:** 9,863 records / 23 islands ✓ (stable across filter iterations)

---

## Type-2 Contamination Diagnostic

Records kept **only** by condition C (stateProvince=Galápagos, no lat/lon / GADM / locality text / English name) represent the highest-risk category. A diagnostic block in the R script prints their island distribution and top species.

**Finding (2026-05-15 run):** The top species in this category are almost entirely Galápagos endemics (*Geospiza fuliginosa*, *G. fortis*, *Mimus parvulus*, *Camarhynchus parvulus*, *Platyspiza crassirostris*, etc.), confirming these are genuine museum specimens with minimal provenance data — not mainland contamination. Condition C is doing legitimate work for this group.

**One species to watch:** *Pyrocephalus rubinus* (430 condition-C-only records). The Galápagos population (*P. nanus*) has been recently split and is now considered extinct. Older vouchers legitimately carry the mainland name. Records with `year < 1980` are almost certainly genuine historical Galápagos specimens; more recent records warrant review.

---

## species_by_island.R

Reads `galapagos_specimens.tsv` and `galapagos_unresolved.tsv` and produces per-class (Aves, Mammalia, Testudines, Squamata) species × island matrices:

- `<class>_record_counts.tsv` — number of specimens per species per island
- `<class>_last_year.tsv` — most recent collection year per species per island

The "archipelago" column in these tables comes from `galapagos_unresolved.tsv` (records confirmed Galápagos but not resolved to a specific island).

A diagnostic block (added 2026-05-14) prints records where `best` is NA/empty/"-" in the specimens dataframe, flagging which filter conditions they satisfy — useful for tracing any future upstream filter gaps.

---

## Known Issues and Limitations

### 1. Taxonomic synonymy across the Galápagos/mainland boundary
GBIF's taxonomic backbone normalizes names globally but does not account for geography-specific splits. Species recently split from mainland relatives (Galápagos mockingbirds, Darwin's finches, marine iguanas, tortoises) have historical records under the old lumped name. A Galápagos-specific taxonomic thesaurus is needed to fully resolve this. Proposed approach:
- Use the Charles Darwin Foundation (CDF) species checklist as backbone
- Build a lookup table: `accepted_name` → `galapagos_status` (endemic / resident / visitor / vagrant / introduced / extirpated) + `expected_islands`
- Use GBIF's `acceptedScientificName` field to map old names through their backbone before querying the CDF thesaurus
- Flag (not hard-filter) records where the species × island combination is unexpected

### 2. County field ambiguity
`county` in GBIF Ecuador records can refer to either a Galápagos island (legitimate) or a mainland administrative canton with the same Spanish name (e.g., Santa Cruz canton in Guayas, Santiago canton). The name resolver now consults `county` only after `island`, `locality`, `verbatimLocality`, and `islandGroup` have all failed to match, reducing but not eliminating this risk.

### 3. Morona-Santiago → Santiago false positives
The name resolver matches "Santiago" within "Morona-Santiago" (a mainland province) to Santiago Island. These are all caught by the Step 3 provenance filter (Morona-Santiago stateProvince fails conditions A–E), but the 14,195 records being processed and dropped is computationally wasteful. A future improvement could add a pre-filter blocklist for known mainland province names.

### 4. `best == ""` gap in Step 1 filter
The Step 1 filter `filter(best != "-", !is.na(best))` would pass records where `best` is an empty string `""`. Although `analyze.py` should never write `""` (it uses `best.loc or "-"`), the filter has been updated to also check `best != ""` as a defensive measure.

### 5. `species_by_island.R` does not filter on `best`
The script loads `galapagos_specimens.tsv` and uses the `best` column directly without checking for NA/empty values. A diagnostic block flags these if they occur. The underlying fix should always be in the upstream pipeline script.

---

## Development History

| Date | Commit | Change |
|---|---|---|
| 2023-07-11 | `c6c9c54` | Initial commit |
| 2023-11-08 | `82e4732` | Improved analysis system (latlon + name resolvers) |
| 2024-01-23 | `d6471bc` | Sophisticated island name resolver; fix Española coastline |
| 2024-04-05 | `7b99a0c` | Name resolver stops at first column with a match |
| 2024-04-14 | `b2c33b0` | Em-dash/en-dash handling; `--` phrase splitting; more named places |
| 2024-04-17 | `d1577ac` | Add `county` field to name resolver |
| 2024-04-17 | `4e0f0eb` | Fix ParserError on bad lines; suppress Python 3.12 SyntaxWarnings |
| 2026-04-18 | `f994fd4` | Add R pipeline scripts (Pipelines A and B) to repo |
| 2026-04-18 | `dce65d2` | Fix Pipeline B (gbif_data_ingester.R) Sections 10–12 |
| 2026-04-18 | `395db5c` | Stop tracking runtime outputs (results.tsv etc.) in git |
| 2026-04-19 | `a70038e` | **Add GADM condition B; remove stateProvince=NA (former condition C)** — eliminated ~176K mainland false positives |
| 2026-04-19 | `dd1c597` | Add county/remarks to condition D; add English island names as condition E — recovered ~1,600 pre-GPS museum records |
| 2026-04-20 | `4ef16bc` | Add Pipeline C (GADM ECU.9_1 ground-truth download) |
| 2026-04-21 | `c91daee` | Add `load_galapagos_data.R` helper |
| 2026-05-12 | `8472e7a` | Tighten unresolved-record filter (require B/D/E in addition to C); add type-2 contamination diagnostic |
| 2026-05-14 | `a281dba` | Minor R script updates |
| 2026-05-15 | `662decc` | Add `stateProvince` to name resolver (adj=-1); add best-NA diagnostic to species_by_island.R; add `"sta cruz"` alias in islands.py |
| 2026-05-15 | `6735c87` | Add `islandGroup`, `locationRemarks`, `occurrenceRemarks` to name resolver; move `county` to last among adj=0 fields |

---

## Re-running the Pipeline

To regenerate output from scratch after changes to `analyze.py`:

```bash
# 1. From the repo root, run island assignment on the Ecuador input TSV
bash analyze.sh ~/Dropbox/Galapagos_data/input/ecuador_occurrences.tsv

# 2. In R, source the download/filter script
source("r/gbif_ecuador_download.R")   # Pipeline A
source("r/gbif_data_ingester.R")      # Pipeline B (if multi-pull data is current)

# 3. Load all outputs into a clean environment
source("r/load_galapagos_data.R")

# 4. Generate species × island tables
source("r/species_by_island.R")
```

To check pipeline health without a full re-run, look for:
- **CAS Aves count** (~9,860 records / 23 islands) — the primary sanity check
- **Type-2 diagnostic output** — should show Galápagos endemic species, not mainland ones
- **best-NA diagnostic in species_by_island.R** — should print "All specimen records have a valid island in 'best'"

---

*Generated 2026-05-15. For questions, contact Jack Dumbacher (jdumbacher@calacademy.org).*
