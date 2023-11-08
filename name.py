import unicodedata

from base import *
import islands

def normalize(s):
	return unicodedata.normalize("NFKD", s.casefold()).encode("ASCII", "ignore").decode()

class NameResolver(Resolver):
	name = "name"
	name_columns = ["island", "municipality", "locality", "verbatimLocality", "islandGroup", "locationRemarks"]

	def __init__(self):
		self.aliases = {}
		for island in islands.islands:
			for alias in island.aliases: self.aliases[alias] = island.name

	def try_match(self, s):
		normal = normalize(s)
		for name in islands.names:
			if name in normal: return name
		for (alias, canonical) in self.aliases.items():
			if alias in normal: return canonical
		return None

	def resolve(self, row):
		for col in self.name_columns:
			if row[col] is None: continue
			name = self.try_match(row[col])
			if name is not None: return Resolution(name, HIGH, self.name)
		return UNKNOWN

def test():
	return True
