import logging
import xml.etree.ElementTree

from base import *
import islands

# Names known to be present in GBIF data, mapped to the equivalent species as presented in the IOW data.
synonyms = {
	"Oceanodroma castro": "Hydrobates castro",
	"Aphriza virgata": "Calidris virgata",
	"Oceanodroma leucorhoa": "Hydrobates leucorhous",
	"Phalacrocorax harrisi": "Nannopterum harrisi",
	"Puffinus creatopus": "Ardenna creatopus",
	"Philomachus pugnax": "Calidris pugnax",
	"Anas clypeata": "Spatula clypeata",
	"Anas cyanoptera": "Spatula cyanoptera",
	"Aratinga erythrogenys": "Psittacara erythrogenys",
	"Anas discors": "Spatula discors",
	"Puffinus pacificus": "Ardenna pacifica",
	"Puffinus griseus": "Ardenna grisea",
	"Charadrius wilsonia": "Anarhynchus wilsonia",
	"Tryngites subruficollis": "Calidris subruficollis",
	"Oceanodroma tethys": "Hydrobates tethys",
	"Laterallus spilonotus": "Laterallus spilonota",
	"Neocrex erythrops": "Mustelirallus erythrops",
	"Oceanodroma markhami": "Hydrobates markhami",
	"Oceanodroma hornbyi": "Hydrobates hornbyi",
	"Oceanodroma microsoma": "Hydrobates microsoma",
}

class TaxonomicDatabase:
	def __init__(self, file):
		self.tree = xml.etree.ElementTree.parse(file).getroot()[0]

	def iter(self):
		def n(x): return x.find("latin_name").text
		for order in self.tree.findall("order"):
			for family in order.findall("family"):
				for genus in family.findall("genus"):
					for species in genus.findall("species"):
						yield (n(order), n(family), n(genus), n(species))

	def ordering(self):
		ret = {}
		for (i, (_, _, genus, species)) in enumerate(self.iter()):
			ret[f"{genus} {species}"] = i
		return ret

class ObservationMapper:
	"""Builds a table of species observed on each island for taxa of particular interest.

	This manages species of interest and records observation counts for each species-island pair, to be written to a summary table.
	"""

	classes_of_interest = {"Aves"}

	def __init__(self, dbfile):
		self.observations = {}
		self.db = TaxonomicDatabase(dbfile)

	def should_include(self, row):
		return row.get("class", "") in self.classes_of_interest

	def add(self, row, island):
		if not self.should_include(row): return
		species = row.get("species", "")
		if species == "": return
		if species in synonyms: species = synonyms[species]
		self.observations.setdefault((species, island), set()).add(row["gbifID"])

	def summarize(self):
		ordering = self.db.ordering()
		observed_species = set(obs[0] for obs in self.observations.keys())
		unknown_species = observed_species - set(ordering.keys())
		if len(unknown_species) > 0:
			logging.warning("Ignoring species not in taxonomic database:")
			for species in unknown_species: logging.warning(f"    {species}")
			observed_species -= unknown_species
		sorted_species = sorted(observed_species, key=lambda x: ordering[x])
		sorted_islands = sorted(island.name for island in islands.islands)
		table = { k: len(v) for (k, v) in self.observations.items() }
		return Table(table, sorted_species, sorted_islands, "")
