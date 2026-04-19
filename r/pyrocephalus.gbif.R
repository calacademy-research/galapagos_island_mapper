#############################################################
## read.gbif.R -- 
## read in gbif search results from 2022
## Script to get in huge dataset of gbif data
## Clean it up and perform some initial descriptive analyses
## jdumbacher@calacademy.org - September 2022
## ------------------------------------------------------------

#some requirements for this script
#install.packages("tidyverse")
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

# read tab-delimimted text file, maintaining column names, in as a tibble named "ebd_data"
pyrocephalus_data_all <- read_delim("/Users/jdumbacher/pyrocephalus/specimens/input/0048054-230530130749713/occurrence.txt",delim='\t',)
pyrocephalus_Galapagos <- read_delim("/Users/jdumbacher/pyrocephalus/specimens/input/0048054-230530130749713/Galap_occurrence_head.txt",delim='\t',)

pyro_data <- pyrocephalus_data_all %>%
  select(institutionCode, catalogNumber, stateProvince,	municipality,	locality,	verbatimLocality,	decimalLatitude, decimalLongitude, scientificName, genus, specificEpithet, acceptedScientificName, basisOfRecord, year, eventDate, municipality, countryCode ) %>% 
  filter(!(institutionCode=="iNaturalist") , !(institutionCode=="CLO"), !(is.na(institutionCode))) %>% 
  arrange(locality)  #%>%
# filter(stateProvince == "Galapagos" | stateProvince == "Galápagos")
names(pyro_data) <- make.names(names(pyro_data),unique = TRUE)

galap_pyro_data <- pyrocephalus_Galapagos %>%
  select(institutionCode, catalogNumber, stateProvince,	municipality,	locality,	verbatimLocality,	decimalLatitude, decimalLongitude, scientificName, genus, specificEpithet, acceptedScientificName, basisOfRecord, year, eventDate, municipality, countryCode ) %>% 
  filter(!(institutionCode=="iNaturalist") , !(institutionCode=="CLO"), !(is.na(institutionCode))) %>% 
  arrange(locality)  #%>%
# filter(stateProvince == "Galapagos" | stateProvince == "Galápagos")
names(galap_pyro_data) <- make.names(names(galap_pyro_data),unique = TRUE)



pyro_cas <-pyro_data %>% 
  filter(institutionCode=="CAS")

# replace spaces in variable names, so that they can be used later:
names(pyro_data) <- make.names(names(pyro_data),unique = TRUE)

localities <- unique(pyro_data$locality)
localities

galap_localities <- unique(galap_pyro_data$locality)
galap_localities

select_record <- galap_pyro_data %>% 
  filter(locality == "Barrington I")
select_record

isle_names <- read.csv("/Users/jdumbacher/pyrocephalus/specimens/input/Galapagos_island_names.csv")

Rabida_recs <- "Rabida" %in% galap_pyro_data$locality

for (i in 1:35) {
  pyro_data$isle <- if (grepl(paste0(isle_names$old_name[i]), pyro_data$locality)) {  
    paste0(isle_names$new_name[i]) }
}
 

 
# if (grepl("Indefatigable", pyro_data$locality)) {pyro_data$isle = "Isla Santa Cruz"}

  # pyro_data$isle <- ifthen(grepl(paste0(isle_names$old_name[1]), pyro_data$locality), paste0(isle_names$new_name[1]))

unique(pyro_data$isle)
isle_names$new_name[1]
isle_names$old_name[1]

ebd_data %>%
  mutate(DATE=ymd(OBSERVATION.DATE)) %>%
  mutate(YEAR = year(DATE)) %>%
  select(COMMON.NAME,SCIENTIFIC.NAME, OBSERVATION.COUNT, LATITUDE, LONGITUDE, OBSERVATION.DATE, YEAR) -> ebd_short_data

ebd_short_data %>%
  filter(COMMON.NAME=="Barred Owl") -> BADO_data

owls <- c("Barred Owl", "Spotted Owl", "Northern Pygmy-Owl", "Burrowing Owl", "Western Screech-owl", "Long-eared Owl", "Short-eared Owl", "Northern Saw-whet Owl")
ebd_short_data %>%
  filter(COMMON.NAME %in% owls) -> ebd_owl_data

ebd_owl_data %>%
  group_by(COMMON.NAME, YEAR) %>%
  summarize(n()) %>%
  select(COMMON.NAME, YEAR, N="n()") -> owl_summ

t <- ggplot (data = owl_summ) +
  geom_point(mapping = aes(x=YEAR, y=N))
t + facet_grid(COMMON.NAME~.)

write_tsv(ebd_owl_data, file.path(paths_to_directory,"ebd_owl_data.txt"), col_names = TRUE)
