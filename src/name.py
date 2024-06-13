import Levenshtein as levenshtein
import re
import unicodedata

from base import *
import islands

def normalize(s):
	return unicodedata.normalize("NFKD", s.casefold()).encode("ASCII", "ignore").decode()

# Simple dict wrapper for tracking and manipulating relevance scores for islands
class ScoreMap:
	def __init__(self):
		self.scores = {}

	def __len__(self): return len(self.scores)

	def names(self): return self.scores.keys()

	def add(self, name, score):
		if name not in self.scores or score > self.scores[name]: self.scores[name] = score

	def merge(self, other):
		for (name, score) in other.scores.items(): self.add(name, score)

	def inc(self, name, amount=1):
		if name in self.scores: self.scores[name] += amount

	def dec(self, name, amount=1):
		if name in self.scores: self.scores[name] -= amount
	
	def incall(self, amount=1):
		for name in self.scores: self.scores[name] += amount

	def decall(self, amount=1):
		for name in self.scores: self.scores[name] -= amount

	def keep_best(self):
		if self.scores == {}: return
		hi = max(self.scores.values())
		remove = set()
		for (name, score) in self.scores.items():
			if score < hi: remove.add(name)
		for name in remove: del self.scores[name]
	
	def resolutions(self):
		ret = []
		for (name, score) in self.scores.items():
			conf = MODERATE
			if score > 7: conf = HIGH
			elif score < 3: conf = LOW
			ret.append(Resolution(name, conf, NameResolver.name))
		return ret

class NameResolver(Resolver):
	"""Resolve observations to islands based on island names in location fields.

	This resolver pulls out the `name_columns` listed below and splits each one into "phrases", then searches for island names in
	each phrase.  Using some heuristics like the presence of certain prepositions, we guess at whether each occurrences is the island
	name where the observation occurred, or just a false positive (such as the name of a non-island feature, or the name of a nearby
	island to help find the intended one).  The first column that matches a name is returned.
	"""

	name = "name"
	name_columns = {"island": +1, "locality": 0, "verbatimLocality": 0} #, "level3Name": -1, "level2Name": -1}
	island_words = ["island", "islet", "isla", "isl", "is", "id", "i", "roca"]
	# TODO Consider handling "between ... and ..."
	suspicious_prepositions = {"off", "also", "by", "near", "toward", "to"}
	place_modifiers = {"bay", "punta", "point", "bahia", "playa", "beach", "volcano", "volcan", "barrio", "cerro", "canal", "harbor"}

	def __init__(self):
		# List of island names and aliases, split into words
		self.name_parts = []
		# Mapping from split island names and aliases to canonical names
		self.name_resolve = {}
		for island in islands.islands:
			parts = island.name.split(" ")
			self.name_parts.append(parts)
			self.name_resolve[tuple(parts)] = island.name
			for alias in island.aliases:
				parts = alias.split(" ")
				self.name_parts.append(parts)
				self.name_resolve[tuple(parts)] = island.name
		# Put names with more words first, so we match the longest possible name
		self.name_parts.sort(key=len, reverse=True)

	# Return a list containing one tuple per island name occurring in the phrase, where each tuple is (island name, prefix words, suffix words).
	def parse_phrase(self, s):
		words = re.split("\\W+", s)
		if words == []: return []

		# Find occurrences of island names in the strings.
		occurrences = []
		interstitial = []
		i = 0
		while i < len(words):
			match = False
			for name in self.name_parts:
				candidate = " ".join(words[i:i + len(name)])
				if candidate in self.place_modifiers: continue
				distance = levenshtein.distance(candidate, " ".join(name))
				# We've special-cased a couple common distance-2 misspellings in islands.py as well.
				if distance <= 1:
					if occurrences != []:
						occurrences[-1][2].extend(interstitial)
					occurrences.append((self.name_resolve[tuple(name)], interstitial, [], -2 * distance))
					interstitial = []
					i += len(name)
					match = True
					break
			if not match:
				interstitial.append(words[i])
				i += 1
		if occurrences == []: return []
		else: occurrences[-1][2].extend(interstitial)
		return occurrences

	def score_occurrence(self, prefix, suffix):
		if len(prefix) > 0:
			if prefix[-1] in self.island_words: prefix = prefix[:-1]
			elif prefix[-1] in self.place_modifiers: return 0
		if len(suffix) > 0:
			if suffix[0] in self.island_words: suffix = []
			elif suffix[0] in self.place_modifiers: return 0

		if prefix == [] and suffix == []: return 8
		for word in prefix:
			if word in self.suspicious_prepositions: return 2
		if suffix != []: return 4
		return 6

	def split_phrases(self, s):
		# Ideally, we would avoid splitting on periods that are part of an abbreviation for "island", but that seems like a lot of work for minimal gain.
		for part in re.split("[,.;\\(\\)\\[\\]\\|]+", s):
			if part == "": continue
			yield part.strip()

	def special_cases(self, island, prefix, suffix):
		# Darwin Research Station is on Santa Cruz Island
		if island == "darwin" and "station" in suffix: return ("santa cruz", 2)
		#if island == "santa cruz": return -2
		return (None, 0)

	def resolve(self, row):
		for (col, adj) in self.name_columns.items():
			val = row.get(col, "")
			if val == "": continue
			col_results = ScoreMap()
			for phrase in self.split_phrases(normalize(val)):
				phrase_results = ScoreMap()
				for (island, prefix, suffix, adjustment) in self.parse_phrase(phrase):
					score = self.score_occurrence(prefix, suffix) + adjustment
					(island_override, score_adj) = self.special_cases(island, prefix, suffix)
					score += score_adj
					if island_override is not None: island = island_override
					#print(f"({prefix}, {island}, {suffix}) -> {score}")
					if score > 0: phrase_results.add(island, score)
				if len(phrase_results) > 1: phrase_results.decall()
				col_results.merge(phrase_results)
			if len(col_results) > 0:
				col_results.keep_best()
				col_results.incall(adj)
				return col_results.resolutions()
		return []

name_tests = [
	# Synthetic tests
	({"locality": "", "verbatimLocality": "", "island": "seymour"}, {"seymour"}),
	({"locality": "", "verbatimLocality": "", "island": "south seymour"}, {"baltra"}),
	({"locality": "", "verbatimLocality": "", "island": "north seymour"}, {"seymour"}),
	({"locality": "", "verbatimLocality": "", "island": "gardner off charles island"}, {"gardner"}),
	({"locality": "", "verbatimLocality": "", "island": "gardner off floreana island"}, {"gardner"}),
	({"locality": "", "verbatimLocality": "", "island": "gardner off espanola island"}, {"gardner"}),
	# Organic tests from actual data
	(
		{
			"locality": "genovesa (tower island); darwin bay",
			"verbatimLocality": "ecuador | galápagos | genovesa (tower island); darwin bay",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "isla baltra (also known as south seymour island), 650 yards e of punta noboa",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra"}
	),
	(
		{
			"locality": "santa cruz island, baltra island",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "baltra"}
	),
	(
		{
			"locality": "isabela island, santa cruz island",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "isabela"}
	),
	(
		{
			"locality": "galapagos islands, baltra island, small cove at northeast end of boat landing about 700 meters east of punta noboa",
			"verbatimLocality": "",
			"island": "south seymour island (baltra)",
		},
		{"baltra"}
	),
	(
		{
			"locality": "santa cruz--los gemelos and south plaza isla",
			"verbatimLocality": "",
			"island": "",
		},
		{"plaza"}
	),
	(
		{
			"locality": "off indefatigable",
			"verbatimLocality": "south america, ecuador, off indefatigable",
			"island": "isla baltra",
		},
		{"baltra"}
	),
	(
		{
			"locality": "narborough opp.tagos cove,albemarle id,galapagos is",
			"verbatimLocality": "narborough opp. tagos cove,albemarle id,galapagos is",
			"island": "",
		},
		{"fernandina", "isabela"}
	),
	(
		{
			"locality": "galapagos. [insularum chatham et charles (=san cristobal and floreana islands)].",
			"verbatimLocality": "galapagos. [insularum chatham et charles (=san cristobal and floreana islands)].",
			"island": "",
		},
		{"san cristobal", "floreana"}
	),
	(
		{
			"locality": "arrived and departed caleta del norte, baltra island, n of santa cruz island, galapagos",
			"verbatimLocality": "",
			"island": "south seymour island (baltra)",
		},
		{"baltra"}
	),
	(
		{
			"locality": "between fernandina and isabela islands",
			"verbatimLocality": "",
			"island": "",
		},
		{"fernandina", "isabela"}
	),
	(
		{
			"locality": "jervis island (=isla rábida), south of isla santiago and northwest of isla santa cruz, on the last promontory of the island",
			"verbatimLocality": "",
			"island": "",
		},
		{"rabida"}
	),
	(
		{
			"locality": "caleta del norte, baltra island, n of santa cruz island",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra"}
	),
	(
		{
			"locality": "between isla fernandina (narborough island) & isla santa cruz (indefatigable island)",
			"verbatimLocality": "ecuador | galápagos | e trop pacific ocean | between isla fernandina (narborough island) & isla santa cruz (indefatigable island)",
			"island": "",
		},
		{"fernandina", "santa cruz"}
	),
	(
		{
			"locality": "canal de itabaca",
			"verbatimLocality": "ecuador, galapagos islands, isla baltra, itabaca",
			"island": "south seymour island",
		},
		{"baltra"}
	),
	(
		{
			"locality": "galapagos islands, baltra island, west end of boat landing about 650 yards east of point noboa, edge of cement pier or boat landing",
			"verbatimLocality": "",
			"island": "south seymour island (baltra)",
		},
		{"baltra"}
	),
	(
		{
			"locality": "eden island, off indefatigable island",
			"verbatimLocality": "",
			"island": "",
		},
		{"eden"}
	),
	(
		{
			"locality": "isla baltra, w side of, n of isanta cruz island",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra"}
	),
	(
		{
			"locality": "isla baltra (also known as south seymour island)",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra"}
	),
	(
		{
			"locality": "galapagos - on side of rocky cliff at 50 ft. altitude. indefatigable island.specimens of this same species were found on charles island.",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "floreana"}
	),
	(
		{
			"locality": "baltra to isla seymour",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra", "seymour"}
	),
	(
		{
			"locality": "eden island, indefatigable island",
			"verbatimLocality": "",
			"island": "",
		},
		{"eden", "santa cruz"}
	),
	(
		{
			"locality": "south plaza, santa fe islands, galapagos",
			"verbatimLocality": "",
			"island": "",
		},
		{"plaza", "santa fe"}
	),
	(
		{
			"locality": "isla santa cruz; bartolomé",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "bartolome"}
	),
	(
		{
			"locality": "off e end of isla salvador. [isla san bartolomé = isla bartolomé (bartholomew i.)]",
			"verbatimLocality": "",
			"island": "bartolomé [bartholomew]",
		},
		{"bartolome"}
	),
	(
		{
			"locality": "between s seymour & daphne islands, james island, galapagos",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra", "daphne", "santiago"}
	),
	(
		{
			"locality": "off santa cruz island, isla el edén, rock pools",
			"verbatimLocality": "",
			"island": "",
		},
		{"eden"}
	),
	(
		{
			"locality": "charles island, onslow island, near post office bay, inside crater",
			"verbatimLocality": "",
			"island": "",
		},
		{"floreana", "onslow"}
	),
	(
		{
			"locality": "genovesa (tower) island, darwin bay, lagoon beach",
			"verbatimLocality": "",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "between fernandina and isabela ids.",
			"verbatimLocality": "",
			"island": "",
		},
		{"fernandina", "isabela"}
	),
	(
		{
			"locality": "tower island, darwin bay",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | tower island, darwin bay",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "darwin bay",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | darwin bay",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "darwin bay, anchorage",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | darwin bay, anchorage",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "between north & south plaza is. off east side santa cruz island",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | between north & south plaza is. off east side santa cruz island",
			"island": "",
		},
		{"plaza"}
	),
	(
		{
			"locality": "no specific locality recorded., santa cruz island",
			"verbatimLocality": "galapagos islands; 25 m. so of santa cruz; barrington island",
			"island": "",
		},
		{"santa cruz", "santa fe"}
	),
	(
		{
			"locality": "isla isabela, w of isla guy fawkes",
			"verbatimLocality": "",
			"island": "",
		},
		{"isabela", "guy fawkes"}
	),
	(
		{
			"locality": "galapagos islands, james (san salvador) island.  sullivan bay located on the ne side of the island.",
			"verbatimLocality": "",
			"island": "",
		},
		{"santiago"}
	),
	(
		{
			"locality": "bartholomew id., sullivan bay (james island)",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands | santiago island | bartholomew id., sullivan bay (james island)",
			"island": "",
		},
		{"bartolome", "santiago"}
	),
	(
		{
			"locality": "baltra channel to south plaza island (boat ride)",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra", "plaza"}
	),
	(
		{
			"locality": "isla santa cruz, galapagos islands, isla floreana (isla santa maria, charles island)",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "floreana"}
	),
	(
		{
			"locality": "isla santa cruz, galapagos, ecuador",
			"verbatimLocality": "bahia academy, darwin research station",
			"island": "bahia academy, darwin research station",
		},
		{"santa cruz", "darwin"}
	),
	(
		{
			"locality": "galapagos, ecuador",
			"verbatimLocality": "tower island, darwin bay; rock; r/v velero iii, station 782-38.",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "isla santa maria & isla campeon, 9-18 m, sand & profilic coral, night dive",
			"verbatimLocality": "",
			"island": "",
		},
		{"floreana", "champion"}
	),
	(
		{
			"locality": "2 km from w end of isla san bartolome, off e coast of isla san salvador.",
			"verbatimLocality": "",
			"island": "",
		},
		{"bartolome", "santiago"}
	),
	(
		{
			"locality": "galapagos is., isla baltra, caleta del norte, n of isla santa cruz, anton bruun cr. 18b, sta. 791",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra", "santa cruz"}
	),
	(
		{
			"locality": "tower island, darwin bay, n.w. shore tide pools",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | tower island, darwin bay, n.w. shore tide pools",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "bartholomew i., near san salvador i. (near james id.)",
			"verbatimLocality": "",
			"island": "",
		},
		{"bartolome"}
	),
	(
		{
			"locality": "isla santa fe/isla plaza sur, galapagos, ecuador",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa fe", "plaza"}
	),
	(
		{
			"locality": "ec-galapagos--isla rábida -> isla eden",
			"verbatimLocality": "",
			"island": "",
		},
		{"rabida", "eden"}
	),
	(
		{
			"locality": "genovesa [tower] island, darwin bay",
			"verbatimLocality": "",
			"island": "",
		},
		{"genovesa"}
	),
	(
		{
			"locality": "isla santa cruz; santiago, se of the island opposite sombrero chino",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "santiago"}
	),
	(
		{
			"locality": "galapagos, island santa cruz, roca sin nombre",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "sin nombre"}
	),
	(
		{
			"locality": "between daphne island & seymour island",
			"verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | between daphne island & seymour island",
			"island": "",
		},
		{"daphne", "seymour"}
	),
	(
		{
			"locality": "caleta del norte, isla baltra (n of isla santa cruz), [ca. 22.2 air miles n puerto ayora]",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "baltra"}
	),
	(
		{
			"locality": "",
			"verbatimLocality": "gardner isl., (near charles) galapagos arch.",
			"island": "gardner-by-floreana id.",
		},
		{"gardner"}
	),
	(
		{
			"locality": "indefagitable island, tortuga bay",
			"verbatimLocality": "",
			"island": "indefagitable id.",
		},
		{"santa cruz"}
	),
	(
		{
			"locality": "ablemarle island, cowley bay",
			"verbatimLocality": "",
			"island": "ablemarle id.",
		},
		{"isabela"}
	),
	(
		{
			"locality": "",
			"verbatimLocality": "",
			"island": "south seymour",
		},
		{"baltra"}
	),
	(
		{
			"locality": "north seymour",
			"verbatimLocality": "",
			"island": "",
		},
		{"seymour"}
	),
	(
		{
			"locality": "off isla el edén, nw santa cruz island(indefatigable island)",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz"}
	),
	(
		{
			"locality": "",
			"verbatimLocality": "hp4j+4c3 airport of the galapagos islands, aeropuerto seymour de baltra, isla baltra baltra, seymour, équateur",
			"island": "",
		},
		{"baltra", "seymour"}
	),
	(
		{
			"locality": "baltra airport santa cruz island, galapogos, parking lot",
			"verbatimLocality": "",
			"island": "",
		},
		{"santa cruz", "baltra"}
	),
	(
		{
			"locality": "galapagos islands, daphne major islet lying n",
			"verbatimLocality": "",
			"island": "",
		},
		{"daphne"}
	),
	(
		{
			"locality": "galapagos islands. charles (floreana) island. actually collected on champion islet lying abut 1 kilometer offshore and ne of post office bay.",
			"verbatimLocality": "",
			"island": "",
		},
		{"floreana", "champion"}
	),
	(
		{
			"locality": "north plazas islet off e shore of santa cruz island",
			"verbatimLocality": "",
			"island": "",
		},
		{"plaza"}
	),
	(
		{
			"locality": "Isla Santa Cruz, Galapagos, Ecuador",
			"verbatimLocality": "Darwin Research Station, Academy Bay [with dried body]",
			"island": "Bahia Academy, Darwin Research Station",
		},
		{"santa cruz"}
	),
	(
		{
			"locality": "S Seymour I.",
			"verbatimLocality": "",
			"island": "",
		},
		{"baltra"}
	),
]

def test():
	resolver = NameResolver()
	failed = 0
	for (test, expected) in name_tests:
		results = { res.loc for res in resolver.resolve(test) if res.loc is not None and res.conf != LOW }
		unexpected = results - expected
		if results == set() or unexpected != set():
			print(f"Test failure: extracting location name for {test!r} yielded {results!r} not completely in expected results {expected!r}")
			failed += 1
	if failed > 0: print(f"Failed {failed} of {len(name_tests)} tests")
	return failed == 0
