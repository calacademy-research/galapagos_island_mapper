# =========================================================
# Load Galápagos Pipeline Outputs into Clean Dataframes
# =========================================================
# Reads the final output files from all three pipelines into
# a tidy set of named dataframes.  Run this after all three
# pipelines have been executed to get a clean working environment.
#
# Dataframes loaded:
#   specimens_ecuador    — gbif_ecuador_download.R    (single current pull)
#   unresolved_ecuador   — gbif_ecuador_download.R    (unresolved records)
#   specimens_multipull  — gbif_data_ingester.R       (multi-pull, deduplicated)
#   unresolved_multipull — gbif_data_ingester.R       (unresolved records)
#   gadm_all             — gbif_galapagos_gadm_download.R (all GBIF types)
#   gadm_specimens       — gbif_galapagos_gadm_download.R (physical specimens only)
# =========================================================

library(dplyr)
library(readr)

OUTPUT_DIR <- "~/Dropbox/Galapagos_data/output/"

read_pipeline <- function(filename) {
  path <- file.path(OUTPUT_DIR, filename)
  cat(sprintf("  %-48s", filename))
  df <- read_tsv(path,
                 col_types      = cols(.default = col_character()),
                 show_col_types = FALSE)
  cat(sprintf("%7d rows, %d cols\n", nrow(df), ncol(df)))
  df
}

cat("Loading pipeline outputs from:", OUTPUT_DIR, "\n\n")

# ── Pipeline A: single current Ecuador pull ────────────────
cat("Pipeline A — gbif_ecuador_download.R\n")
specimens_ecuador    <- read_pipeline("galapagos_specimens.tsv")
unresolved_ecuador   <- read_pipeline("galapagos_unresolved.tsv")

# ── Pipeline B: multi-pull, deduplicated ───────────────────
cat("\nPipeline B — gbif_data_ingester.R\n")
specimens_multipull  <- read_pipeline("galapagos_specimens_multipull.tsv")
unresolved_multipull <- read_pipeline("galapagos_unresolved_multipull.tsv")

# ── Pipeline C: GADM ECU.9_1 direct download ──────────────
cat("\nPipeline C — gbif_galapagos_gadm_download.R\n")
gadm_all             <- read_pipeline("galapagos_gadm_all.tsv")
gadm_specimens       <- read_pipeline("galapagos_gadm_specimens.tsv")

# ── Summary ───────────────────────────────────────────────
cat("\n")
cat(strrep("─", 60), "\n")
cat(sprintf("%-30s %10s\n", "Dataframe", "Rows"))
cat(strrep("─", 60), "\n")
for (nm in c("specimens_ecuador", "unresolved_ecuador",
             "specimens_multipull", "unresolved_multipull",
             "gadm_all", "gadm_specimens")) {
  cat(sprintf("%-30s %10d\n", nm, nrow(get(nm))))
}
cat(strrep("─", 60), "\n")
cat("Done. Six dataframes ready in environment.\n")

