install.packages("rgbif")
library(rgbif)

# Set your GBIF credentials first (only needed once)
usethis::edit_r_environ()  # add GBIF_USER, GBIF_PWD, GBIF_EMAIL

?download_predicate_dsl

occ_download(
  pred_or(
    pred_like("stateProvince", "*alápagos*"),
    pred_like("stateProvince", "*alapagos*"),
#    pred_like("island", "*alápagos*"),
#    pred_like("county", "*alapagos*"),
#    pred_like("municipality", "*alápagos*"),
    pred_like("locality", "*alápagos*"),
    pred_like("locality", "*alapagos*"),

#    pred_like("verbatimLocality", "*alápagos*"),
    
    pred_within("POLYGON((-92.3 -1.8, -89.0 -1.8, -89.0 1.9, -92.3 1.9, -92.3 -1.8))")
  ),
  pred("occurrenceStatus", "PRESENT"),
  format = "SIMPLE_CSV"
)
  user   = Sys.getenv("GBIF_USER"),
  pwd    = Sys.getenv("GBIF_PWD"),
  email  = Sys.getenv("GBIF_EMAIL")
)

apr26_data <- occ_download_get('0021094-260409193756587') %>%
  occ_download_import()

meta <- occ_download_meta('0021094-260409193756587')
meta$doi

write_tsv(apr26_data, "~/Dropbox/Galapagos_data/input/occurrence_15APR26.txt")

