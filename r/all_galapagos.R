#############################################################
## read.gbif.R -- 
## read in gbif search results for the Galapagos
## Script to get in huge dataset of gbif data
## Clean it up and perform some initial descriptive analyses
## jdumbacher@calacademy.org - September 2022
##  run by typing:
## Rscript --verbose --vanilla all_galapagos.R
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
library(readr)  # for read_csv()
library(dplyr)  # for mutate()
library(tidyr)  # for unnest()
library(here)   # for here()
library(purrr)  # for map(), reduce()
# library(microbenchmark)
library(stringr) #
library(lubridate) #
library(clipr)
library(rstan)
library(data.table)

# Start with a downloaded search of GBIF data. For this first file, I searched ALL GBIF data from Ecuador.
# Then to filter for Galapagos data, I ran the following command on the command line:
#    grep -e "Galapagos" -e "Galápagos" -e "galapagos" -e "galápagos" occurrence.txt > galapagos.occurrence.txt
# You can also use regex to do something like 
#    grep -E 'Galapagos|Galápagos|galapagos|galápagos|Galapagos|galapaGos|Galapago' occurrence.txt | wc -l
# or
#    grep -E '(G|g)al(a|á)pago(s|S)' occurrence.txt > galapagos_gbif.txt
# although this is not what I did to prepare this particular version of the file.
# in order to choose any records that contained the word Galapagos anywhere in the line, as it is 
# highly inconsistent where each museum puts this (in state, municipality, county, etc.)
# This should capture them all, regardless of where it is entered.  It also will capture Galapagos with or 
# without the capital letter or accent.
#
# Then you need to add back the header to the file, run this code on the command line:
# (head -n 1 occurrence.txt && cat galapagos.occurrence.txt) > galapagos.occurrence.wheader.txt
#
# OR you can do all of these things at once, by typing:
# (head -n 1 occurrence.txt && grep -e "Galapagos" -e "Galápagos" -e "galapagos" -e "galápagos" occurrence.txt) > galapagos.occurrence.txt

# read GBIF tab-delimimted text file, maintaining column names, in as a tibble named "galapagos_data_all"
galapagos_data_all <- fread("/home/jdumbacher/Galapagos_data/input/GBIF_ALL_Ecuador_records/galapagos.occurrence.txt")
names(galapagos_data_all) <- make.names(names(galapagos_data_all),unique = TRUE)
names(galapagos_data_all)

galapagos_admin <- fread("/home/jdumbacher/Galapagos_data/input/GBIF-Galapagos_19_July/occurrence.txt")
names(galapagos_admin) <- make.names(names(galapagos_admin),unique = TRUE)
names(galapagos_admin)
galapagos_admin$endDayOfYear <- as.character(galapagos_admin$endDayOfYear)

galapagos_admin$endDayOfYear <- as.character(galapagos_admin$endDayOfYear)
galapagos <- bind_rows(galapagos_data_all, galapagos_admin)
galapagos <- galapagos[!duplicated(gbifID), ]

cas_birds_galapagos <- galapagos %>% 
  filter(publisher == "California Academy of Sciences") %>% 
  # filter(genus=="Phyllodactylus") %>% 
  filter(class == "Aves") %>% 
  select(class, species, year, institutionCode, decimalLatitude)

# Select only the bird records, and put them into a df named aves_galapagos
aves_galapagos <- galapagos %>%
  # select(institutionCode, catalogNumber, stateProvince,	municipality,	locality,	verbatimLocality,	decimalLatitude, decimalLongitude, scientificName, genus, specificEpithet, acceptedScientificName, basisOfRecord, year, eventDate, municipality, countryCode,class ) %>% 
  # filter(!(institutionCode=="iNaturalist") , !(institutionCode=="CLO"), !(is.na(institutionCode))) %>% 
  filter(grepl("Aves", class, ignore.case = TRUE)) %>% 
  arrange(locality)  #%>%

aves_not_galapagos <- aves_galapagos %>%
  filter(!(stateProvince == "Galapagos" | stateProvince == "Galápagos"))

unique(aves_galapagos$class)
unique(aves_galapagos$locality)

galapagos_with_loc <- galapagos %>% 
  mutate(loc_data = gsub("[[:punct:]]| ", "", paste(islandGroup, island, county, locality, verbatimLocality, municipality, level2Name, level3Name))) %>% 
  mutate(loc_data = iconv(loc_data, from="UTF-8",to="ASCII//TRANSLIT")) %>% 
  mutate(loc_data = tolower(gsub("[[:punct:]]| ", "", loc_data))) 

# iconv(aves_galapagos$loc_data,from="UTF-8",to="ASCII//TRANSLIT") # this is another way to remove accents, but it may leave the accents as apostrophes, etc.

isle_names <- fread("/home/jdumbacher/Galapagos_data/input/Galapagos_island_names.csv")

isle_names <- isle_names %>% 
  mutate(name_pattern = tolower(gsub("[[:punct:]]| ", "", iconv(other_name, from="UTF-8",to="ASCII//TRANSLIT")))) 

# assign modern island names to each entry based upon strings found in locality fields

galapagos_with_loc$island_name <- NA

# Iterate over each row in the dataframe
for (i in 1:nrow(galapagos_with_loc)) {
  # Find the corresponding island value from the isle-names lookup table
  matching_islands <- isle_names$modern_name[sapply(isle_names$name_pattern, grepl, galapagos_with_loc$loc_data[i])]
  
  # Assign the island name to the new column in the dataframe
  if (length(matching_islands) > 0) {
    galapagos_with_loc$island_name[i] <- matching_islands[1]
  }
  # This next section helps track progress, as this is a pretty slow process...
  if ((i %% 500) == 0) { 
    print(i)
  }
}

unique(galapagos_with_loc$island_name)

write_csv(galapagos_with_loc, "/home/jdumbacher/Galapagos_data/output/galapagos_with_loc.txt")

fwrite(galapagos_with_loc, "/home/jdumbacher/Galapagos_data/output/galapagos_with_loc.txt", sep = ",")

galapagos_location <- fread("/home/jdumbacher/Galapagos_data/output/galapagos_with_loc.txt")

species <- "Pyrocephalus"

species_list <- galapagos_with_loc %>% 
  filter(genus == species) 

unique(galapagos_with_loc$island_name)


NA_island <- galapagos_with_loc %>% 
  filter(is.na(island_name))

# now let's do some summary stats...

island_summary <- galapagos_with_loc %>% 
  filter(!(is.na(island_name))) %>% 
  # mutate(genus_species = paste(genus," ",species)) %>% 
  arrange(island_name, species, year) %>% 
  group_by(island_name, species) %>% 
  summarize(year=max(year,na.rm = TRUE))

# This will identify candidates for translocations based upon not having been seen
# or documented for two decades:
candidates <- island_summary



# This will allow a deep-dive into particular species or islands

# define the island and genus here:
genus_to_search <- "Geochelone"
island_to_search <- "Rabida"
species_to_search <- "Geospiza pallida"

# this will filter the data:
filtered <- all_galapagos %>% 
  filter(genus==genus_to_search, island_name==island_to_search) %>% 
  # filter((grepl(species_to_search, scientificName)), island_name==island_to_search) %>% 
  #filter(!(institutionCode=="iNaturalist") , !(institutionCode=="CLO"), !(is.na(institutionCode))) %>% 
  arrange(year) %>%
  select(island_name, genus, species, year, institutionCode, loc_data)

# This will write the filtered data to a spreadsheet:
write_excel_csv2(filtered, paste0("/home/jdumbacher/Galapagos_data/output/", genus_to_search, "_", island_to_search, ".txt"))

CAS <- galapagos_with_loc %>% 
  filter(institutionCode=="CAS") %>% 
  filter(class=="Aves") %>% 
  select(island_name, species, year, institutionCode, loc_data)

galapagos_with_loc %>% 
  unique(island_name)


