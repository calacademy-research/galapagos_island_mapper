# gbif museum lists
#
# get data by either running gbif_ecuador_download.R
# source("~/galapagos_island_mapper/r/gbif_ecuador_download.R")       # 1. ingest the gbif data
# source("~/galapagos_island_mapper/r/build_galapagos_thesaurus.R")   # 2. build the thesaurus
# source("~/galapagos_island_mapper/r/refine_taxonomy.R")             # 3. apply it to specimens
# source("~/galapagos_island_mapper/r/species_by_island.R")           # 4. build the output tables  
#
# and then by using the following:

galapagos_specimens <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/galapagos_specimens_refined.tsv") 

galapagos_specimens_abbrev <- galapagos_specimens %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

darwin_core_verts <- galapagos_specimens %>% 
  filter(class %in% c("Aves","Mammalia","Squamata","Testudines")) %>%  
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, class, species, recordedBy) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

museums <- galapagos_specimens %>% 
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

# deep dive into top collections
mvz <- darwin_core_verts %>% 
  filter(institutionCode == "MVZ") %>% 
  arrange(year)

cas <- darwin_core_verts %>% 
  filter(institutionCode == "CAS") %>% 
  arrange(year)

cas_herps <- darwin_core_verts %>% 
  filter(institutionCode == "CAS") %>% 
  filter(class=="Squamata") %>% 
  arrange(year)

cdf <- darwin_core_verts %>% 
  filter(institutionCode == "CDF") %>% 
  arrange(year)

cdf <- galapagos_specimens %>% 
  filter(institutionCode == "CDF") %>% 
  arrange(year)

cas_tortoises <- darwin_core_verts %>% 
  filter(institutionCode == "CAS") %>% 
  filter(class=="Testudines") %>% 
  arrange(year)

cas_all <- galapagos_specimens %>% 
  filter(institutionCode == "CAS") %>% 
  arrange(year)

cas_cats <- unique(cas_tortoises$catalogNumber)

msb <- galapagos_specimens %>% 
  filter(institutionCode=="MSB") %>% 
  arrange(year)

