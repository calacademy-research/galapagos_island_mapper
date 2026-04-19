# gbif museum lists

# get data by either running gbif_galapagos_summarizer.r or by using the following:
galapagos_data <- fread("/Users/jdumbacher/Dropbox/Galapagos_data/output/galapagos_data.tsv") 

galapagos_specimens <- galapagos_data %>% 
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

darwin_core_verts <- galapagos_data %>% 
  filter(class %in% c("Aves","Mammalia","Squamata","Testudines")) %>%  
  filter(year<2000, !(basisOfRecord=="HUMAN_OBSERVATION")) %>% 
  select(gbifID, publisher, institutionID, institutionCode, basisOfRecord, occurrenceID, catalogNumber, sex, lifeStage, preparations, disposition, decimalLongitude, decimalLatitude, year, species, acceptedScientificName, acceptedTaxonKey, latlon, name, best, class, species, recordedBy) %>% 
  arrange(best, species, institutionID, year) %>% 
  arrange(institutionCode)

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
