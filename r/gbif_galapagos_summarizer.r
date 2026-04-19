#############################################################
## gbif_galapagos_summarizer.R -- 
## read in gbif search results for the Galapagos
## Script to get in huge dataset of gbif data
## Clean it up and perform some initial descriptive analyses
## jdumbacher@calacademy.org - September 2022
##  run by typing:
## Rscript --verbose --vanilla all_galapagos.R
## also - to open jupyter lab on a computer, open a terminal window and type
##   jupyter lab --ip='*' --no-browser
## and follow instructions to open a browser window with Jupyter lab open
## ------------------------------------------------------------

#some requirements for this script
# install.packages("tidyverse")
# install.packages("readr")
# install.packages("dplyr")
# install.packages("tidyr")
# install.packages("here")
# install.packages("purr")
# install.packages("stringr")
# install.packages("lubridate")
# install.packages("clipr")
# install.packages("rstan")
# install.packages("datatable")
library(tidyverse)
require(readr)  # for read_csv()
require(dplyr)  # for mutate()
require(tidyr)  # for unnest()
require(here)   # for here()
require(purrr)  # for map(), reduce()
library(microbenchmark)
library(stringr) #
library(lubridate) #
library(clipr)
library(rstan)
library(data.table)
library('taxize')
library(rgbif)


# Start with a downloaded search of GBIF data. For updates, download GBIF data by searching 
# And All must apply:
#    Administrative areas (gadm.org) == ECU.9_1
#    Occurrence status == present
# download the dataset.  The table to use is the occurrence.txt file
#
# Then use the python program, Galapagos Island Mapper (https://github.com/jdumbacher/galapagos_island_mapper/tree/5e8058829dd1f46fb2110bf9d1f122697aa33d4e)
# to add updated, standardized island names
# and download or point to the results.tsv file

# read GBIF tab-delimimted text file, maintaining column names, in as a tibble named "galapagos_gbif_data_all"
galapagos_gbif_data_all <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/input/occurrence_21JUL2025.txt") 
  # filter(gadm=="Galapagos")
names(galapagos_gbif_data_all) <- make.names(names(galapagos_gbif_data_all),unique = TRUE)
names(galapagos_gbif_data_all)

# get results from the analyze.py script
results <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/input/results_21JUL2025.tsv")
results <- results %>% 
  select(gbifID, best, name, latlon)

# Join the new island name (in the column "best") with the other gbif data
galapagos_data <- inner_join(galapagos_gbif_data_all, results, join_by("gbifID"))
names(galapagos_data)


# check for problems if the results file and data file are not merging 1:1 (should be 0 rows)
galapagos_problems <- anti_join(galapagos_gbif_data_all, results, join_by("gbifID")) 


# save the datasets with added locality data from galapagos_island_mapper
write_tsv(galapagos_data, "/Users/jdumbacher/Dropbox/Galapagos_data/output/Galapagos_data_21JUL2025.tsv")
galapagos_data_25Sept2025 <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/Galapagos_data_25Sept2025.tsv")
galapagos_data_24JUL2024 <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/gbif_galapagos.tsv")
galapagos_data_11AUG2023 <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/galapagos_with_loc.txt") %>% 
  mutate(best=island_name, latlon=island_name, name=island_name)
galapagos_data_21JUL2025 <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/Galapagos_data_21JUL2025.tsv")

# these rows force two variables to be read as character, so that all data pulls can be combined into one large database
galapagos_data_25Sept2025$eventDate <- as.character(galapagos_data_25Sept2025$eventDate)
galapagos_data_24JUL2024$eventDate <- as.character(galapagos_data_24JUL2024$eventDate)
galapagos_data_11AUG2023$eventDate <- as.character(galapagos_data_11AUG2023$eventDate)

galapagos_data_25Sept2025$endDayOfYear <- as.character(galapagos_data_25Sept2025$endDayOfYear)
galapagos_data_24JUL2024$endDayOfYear <- as.character(galapagos_data_24JUL2024$endDayOfYear)
galapagos_data_11AUG2023$endDayOfYear <- as.character(galapagos_data_11AUG2023$endDayOfYear)

# This binds the different pulls from GBIF datasets into one large dataset
# make sure that the newest databases are listed first, so that these are ordered so that older duplicates are discarded
galapagos_data <- bind_rows(galapagos_data_25Sept2025, galapagos_data_24JUL2024, galapagos_data_11AUG2023)

# now we want to reduce these to unique rows:
galapagos_data_unique <- unique(galapagos_data). # but this doesn't actually remove any rows...
galapagos_data_one_per_id <- galapagos_data %>% 
  distinct(gbifID, .keep_all = TRUE)


# set this new, more complete dataset to be the galapagos_data dataframe
galapagos_data <- galapagos_data_one_per_id

write_tsv(galapagos_data, "/Users/jdumbacher/Dropbox/Galapagos_data/output/galapagos_data.tsv")  




galapagos_specimens <- galapagos_data %>% 
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

darwin_core_aves <- galapagos_data %>% 
  filter(class=="Aves") %>% 
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, species) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

darwin_core_verts <- galapagos_data %>% 
  filter(class %in% c("Aves","Mammalia","Squamata","Testudines")) %>%  
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, class, species) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

cas_verts <- darwin_core_verts %>% 
  filter(institutionCode=="CAS")

cas_tortoises <- galapagos_data %>% 
  filter(class=="Testudines") %>% 
  filter(institutionCode=="CAS")
  
n_distinct(darwin_core_aves$acceptedScientificName)
n_distinct(darwin_core_verts$acceptedScientificName)

darwin_core_mammals <- galapagos_data %>% 
  filter(class=="Mammalia") %>% 
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, species) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

n_distinct(darwin_core_mammals$acceptedScientificName)
unique(darwin_core_mammals$acceptedScientificName)

darwin_core_Reptiles <- galapagos_data %>% 
  filter(class=="Squamata") %>% 
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, species) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

unique(galapagos_data$class)
n_distinct(darwin_core_Reptiles$acceptedScientificName)
sort(unique(darwin_core_Reptiles$acceptedScientificName))

darwin_core_turtles <- galapagos_data %>% 
  filter(class=="Testudines") %>% 
  filter(year<1990, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, identifier, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, species) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

island_birds_last <- darwin_core_aves %>% 
  filter(!(is.na(best))) %>% 
  # mutate(genus_species = paste(genus," ",species)) %>% 
  arrange(best, species, year) %>% 
  group_by(best, species) %>% 
  summarize(year=max(year,na.rm = TRUE))

island_birds_last_summary <- island_birds_last %>% 
  pivot_wider(
    names_from = best,
    values_from = year
  )

summary_long <- galapagos_data %>% 
  filter(class=="Aves") %>% 
  group_by(best, species) %>% 
  filter(!(best== "-")) %>%
  # filter(best %in% c("rabida","pinzon","santa fe","floreana", "santiago")) %>% 
  filter(!acceptedScientificName=="") %>% 
  filter(!(acceptedScientificName=="-")) %>%
  filter(!is.na("best")) %>% 
  filter(!is.na("acceptedScientificName")) %>% 
  count()

summary_long_focalbirds <- galapagos_data %>% 
  filter(class=="Aves") %>% 
  group_by(best, species) %>% 
  filter(!(best== "-")) %>%
  filter(best %in% c("rabida","pinzon","santa fe","floreana", "santiago")) %>% 
  filter(!acceptedScientificName=="") %>% 
  filter(!(acceptedScientificName=="-")) %>%
  filter(!is.na("best")) %>% 
  filter(!is.na("acceptedScientificName")) %>% 
  count()

summary_long

island_bird.recs_summary <- summary_long %>% 
  pivot_wider(
    names_from = best,
    values_from = n
  )

focal.island_bird.recs_summary <- summary_long_focalbirds %>% 
  pivot_wider(
    names_from = best,
    values_from = n
  )

island_bird.recs_summary
focal.island_bird.recs_summary

write_tsv(island_bird.recs_summary, "/Users/jdumbacher/Dropbox/Galapagos_data/output/island_bird.recs_summary.tsv")
write_tsv(focal.island_bird.recs_summary, "/Users/jdumbacher/Dropbox/Galapagos_data/output/focal.island_bird.recs_summary.tsv")


museums <- galapagos_data_all %>% 
  filter(year<2015, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  group_by(institutionCode, year) %>%
  arrange(year) %>% 
  count() %>% 
  arrange(year)

museum_summary <- museums %>% 
  pivot_wider(
    names_from = year,
    values_from = n
  ) %>% 
  mutate(total = rowSums(across(2:166), na.rm = TRUE)) %>% 
  arrange(desc(total))

museum_summary$institutionCode


museums_birds <- darwin_core_aves %>% 
  group_by(institutionCode, year) %>%
  arrange(year) %>% 
  count() %>% 
  arrange(year)

museum_summary_birds <- museums_birds %>% 
  pivot_wider(
    names_from = year,
    values_from = n
  ) %>% 
  arrange(institutionCode)

museums_verts <- darwin_core_verts %>% 
  group_by(institutionCode, year) %>%
  arrange(year) %>% 
  count() %>% 
  arrange(year)

museum_summary_verts <- museums_verts %>% 
  pivot_wider(
    names_from = year,
    values_from = n
  )

museum_summary_verts$totals = rowSums(museum_summary_verts[, 2:122], na.rm = TRUE)  

museum_summary_verts <- museum_summary_verts %>% 
  arrange(desc(totals))

write_tsv(museum_summary, "/Users/jdumbacher/Dropbox/Galapagos_data/output/museum_summary.tsv")  
write_tsv(museum_summary_verts, "/Users/jdumbacher/Dropbox/Galapagos_data/output/museum_summary_verts.tsv")  


megaoryzomys <- galapagos_vertebrates %>% 
  filter(genus=="Megaoryzomys") %>% 
  group_by(institutionCode, year) %>%
  arrange(year) %>% 
  count() %>% 
  arrange(year)
  
zenaida <- galapagos_vertebrates %>% 
  filter(genus=="Zenaida") %>% 
  filter("Isabela" %in% island) 
  


write_tsv(museum_summary, "/Users/jdumbacher/Dropbox/Galapagos_data/output/museum_summary.tsv")  
write_tsv(museum_summary_verts, "/Users/jdumbacher/Dropbox/Galapagos_data/output/museum_summary_verts.tsv")  


# This will allow a deep-dive into particular species or islands

# define the island and genus here:
genus_to_search <- "Chelonoidis"
island_to_search <- "pinta"
species_to_search <- "Geospiza pallida"

# this will filter the data:
filtered <- galapagos_data %>% 
  filter(genus==genus_to_search, best==island_to_search) %>% 
  # filter((grepl(species_to_search, scientificName)), island_name==island_to_search) %>% 
  #filter(!(institutionCode=="iNaturalist") , !(institutionCode=="CLO"), !(is.na(institutionCode))) %>% 
  arrange(year) %>%
  select(best, class, genus, species, year, institutionCode)

# This will write the filtered data to a spreadsheet:
write_excel_csv2(filtered, paste0("/Users/jdumbacher/Dropbox/Galapagos_data/output/", genus_to_search, "_", island_to_search, ".txt"))

# This is a good way to create species lists for each island:
santa_fe <- darwin_core_aves %>% 
  filter(name=="santa fe") %>% 
  distinct(species)

pinzon <- darwin_core_aves %>% 
  filter(name=="pinzon") %>% 
  distinct(species)

floreana <- darwin_core_aves %>% 
  filter(name=="floreana") %>% 
  arrange(acceptedTaxonKey) %>% 
  distinct(species)

rabida <- darwin_core_aves %>% 
  filter(name=="rabida") %>% 
  distinct(species)


# for a list of reptiles by island: (can do the same for Mammalia and Testudines)
island_reptiles_last <- galapagos_data %>% 
  filter(class=="Squamata") %>% 
  filter(!(is.na(best))) %>% 
  filter(!(is.na(species))) %>% 
  filter(!(is.na(year))) %>% 
  # mutate(genus_species = paste(genus," ",species)) %>% 
  arrange(best, species, year) %>% 
  group_by(best, species) %>% 
  summarize(year=max(year,na.rm = TRUE))

island_reptiles_last_summary <- island_reptiles_last %>% 
  pivot_wider(
    names_from = best,
    values_from = year
  )

summary_reptiles <- galapagos_data %>% 
  filter(class=="Squamata") %>% 
  group_by(best, species) %>% 
  filter(!(best== "-")) %>%
  filter(!acceptedScientificName=="") %>% 
  filter(!(acceptedScientificName=="-")) %>%
  filter(!is.na("best")) %>% 
  filter(!is.na("acceptedScientificName")) %>% 
  count()

summary_reptiles

island_reptiles.recs_summary <- summary_reptiles %>% 
  pivot_wider(
    names_from = best,
    values_from = n
  )

island_reptiles.recs_summary

write_tsv(island_bird.recs_summary, "/Users/jdumbacher/Dropbox/Galapagos_data/output/island_reptiles.recs_summary.tsv")

# to work with ALL vertebrate taxa, use this to create a df with all vertebrate data:
galapagos_vertebrates <- galapagos_data %>% 
  filter(phylum=="Chordata") %>% 
  filter(class %in% c("Amphibia", "Squamata", "Testudines", "Aves", "Mammalia")) %>% 
  filter(!is.na(best))

# This is a good way to create species lists for each island:
santa_fe <- galapagos_vertebrates %>% 
  filter(best=="santa fe") %>% 
  distinct(species) %>% 
  arrange(species)

pinzon <- galapagos_vertebrates %>% 
  filter(best=="pinzon") %>% 
  distinct(species)

floreana <- galapagos_vertebrates %>% 
  filter(best=="floreana") %>% 
  distinct(species)

rabida <- galapagos_vertebrates %>% 
  filter(best=="rabida") %>% 
  distinct(species)

# or to dive into data from CDF:

cdf <- galapagos_vertebrates %>% 
  filter(publisher == "Charles Darwin Foundation for the Galapagos Islands") %>% 
  arrange(catalogNumber)

write_tsv(cdf, "/Users/jdumbacher/Downloads/cdf_verts.tsv")

# for the specimens in the catalog at CDF, here are some short scripts that output table for specimens of different species
cdf_asio <- cdf %>% 
  filter(species == "Asio flammeus") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_tyto <- cdf %>% 
  filter(family == "Tytonidae") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_buteo <- cdf %>% 
  filter(species == "Buteo galapagoensis") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_pterodroma <- cdf %>% 
  filter(species == "Pterodroma phaeopygia") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_laterallus <- cdf %>% 
  filter(species == "Laterallus spilonotus") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_pyrocephalus <- cdf %>% 
  filter(genus == "Pyrocephalus") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_zenaida <- cdf %>% 
  filter(genus == "Zenaida") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_mimus <- cdf %>% 
  filter(genus == "Mimus") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_setophaga <- cdf %>% 
  filter(genus == "Setophaga") %>% 
#  filter(catalogNumber=="1603")
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

cdf_980 <- cdf %>% 
  filter(catalogNumber=="980")
write_tsv(cdf_980, "/Users/jdumbacher/Downloads/cdf_980.tsv")

cdf_1168 <- cdf %>% 
  filter(catalogNumber=="1168")
write_tsv(cdf_1168, "/Users/jdumbacher/Downloads/cdf_1168.tsv")

cdf_nesoryzomys <- cdf %>% 
  filter(genus == "Nesoryzomys") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")


galapagos_dated <- galapagos_vertebrates %>% 
  arrange(eventDate) %>% 
  select("catalogNumber", "eventDate", "ownerInstitutionCode", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")


gal_megaoryzomys <- galapagos_vertebrates %>% 
  filter(genus=="Megaoryzomys") %>% 
  filter(institutionCode=="ASNHC") %>% 
  select()

publishers <- unique(galapagos_vertebrates$publisher)

cas_oystercatcher <- galapagos_vertebrates %>% 
  filter(publisher == "California Academy of Sciences") %>% 
  filter(species=="Haematopus palliatus") %>% 
  select("catalogNumber", "eventDate", "species", "municipality",	"locality",	"decimalLatitude",	"decimalLongitude",	"latlon", "best")

gygis <- galapagos_vertebrates %>% 
  filter(genus=="Gygis")

gygis <- galapagos_data %>% 
  filter(genus=="Gygis")
