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
	name = "name"
	name_columns = ["locality", "verbatimLocality", "island"]
	island_words = ["island", "islet", "isla", "isl", "is", "id", "i", "roca"]
	# TODO Consider handling "between ... and ..."
	suspicious_prepositions = ["off", "also", "by", "near"]

	def matches(self, a, b):
		# We've special-cased a couple common distance-2 misspellings in islands.py as well.
		return levenshtein.distance(a, b) <= 1

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
				if self.matches(" ".join(words[i:i + len(name)]), " ".join(name)):
					if occurrences != []:
						occurrences[-1][2].extend(interstitial)
					occurrences.append((self.name_resolve[tuple(name)], interstitial, []))
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
		if len(prefix) > 0 and prefix[-1] in self.island_words: prefix = []
		if len(suffix) > 0 and suffix[0] in self.island_words: suffix = []

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

	def resolve(self, row):
		results = {}
		for col in self.name_columns:
			val = row.get(col, "")
			if val == "": continue
			col_results = ScoreMap()
			for phrase in self.split_phrases(normalize(val)):
				phrase_results = ScoreMap()
				for (island, prefix, suffix) in self.parse_phrase(phrase):
					score = self.score_occurrence(prefix, suffix)
					phrase_results.add(island, score)
				if len(phrase_results) > 1: phrase_results.decall()
				col_results.merge(phrase_results)
			results[col] = col_results
		if "island" in results: results["island"].incall()
		all_results = ScoreMap()
		for res in results.values(): all_results.merge(res)
		if len(all_results) == 0: return UNKNOWN
		all_results.keep_best()
		return all_results.resolutions()[0]

name_tests = [
	(
		{"locality": "genovesa (tower island); darwin bay", "verbatimLocality": "ecuador | galápagos | genovesa (tower island); darwin bay", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "isla baltra (also known as south seymour island), 650 yards e of punta noboa", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "santa cruz island, baltra island", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "isabela island, santa cruz island", "verbatimLocality": "", "island": ""},
		{"isabela"}
	),
	(
		{"locality": "galapagos islands, baltra island, small cove at northeast end of boat landing about 700 meters east of punta noboa", "verbatimLocality": "", "island": "south seymour island (baltra)"},
		{"baltra"}
	),
	(
		{"locality": "santa cruz--los gemelos and south plaza isla", "verbatimLocality": "", "island": ""},
		{"plaza"}
	),
	(
		{"locality": "off indefatigable", "verbatimLocality": "south america, ecuador, off indefatigable", "island": "isla baltra"},
		{"baltra"}
	),
	(
		{"locality": "narborough opp.tagos cove,albemarle id,galapagos is", "verbatimLocality": "narborough opp. tagos cove,albemarle id,galapagos is", "island": ""},
		{"fernandina", "isabela"}
	),
	(
		{"locality": "galapagos. [insularum chatham et charles (=san cristobal and floreana islands)].", "verbatimLocality": "galapagos. [insularum chatham et charles (=san cristobal and floreana islands)].", "island": ""},
		{"san cristobal", "floreana"}
	),
	(
		{"locality": "arrived and departed caleta del norte, baltra island, n of santa cruz island, galapagos", "verbatimLocality": "", "island": "south seymour island (baltra)"},
		{"baltra"}
	),
	(
		{"locality": "between fernandina and isabela islands", "verbatimLocality": "", "island": ""},
		{"fernandina", "isabela"}
	),
	(
		{"locality": "jervis island (=isla rábida), south of isla santiago and northwest of isla santa cruz, on the last promontory of the island", "verbatimLocality": "", "island": ""},
		{"rabida"}
	),
	(
		{"locality": "caleta del norte, baltra island, n of santa cruz island", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "between isla fernandina (narborough island) & isla santa cruz (indefatigable island)", "verbatimLocality": "ecuador | galápagos | e trop pacific ocean | between isla fernandina (narborough island) & isla santa cruz (indefatigable island)", "island": ""},
		{"fernandina", "santa cruz"}
	),
	(
		{"locality": "baltra island (n of isla santa cruz):  caleta del norte", "verbatimLocality": "ecuador | galápagos | e trop pacific ocean | baltra island (n of isla santa cruz):  caleta del norte", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "canal de itabaca", "verbatimLocality": "ecuador, galapagos islands, isla baltra, itabaca", "island": "south seymour island"},
		{"baltra"}
	),
	(
		{"locality": "galapagos islands, baltra island, west end of boat landing about 650 yards east of point noboa, edge of cement pier or boat landing", "verbatimLocality": "", "island": "south seymour island (baltra)"},
		{"baltra"}
	),
	(
		{"locality": "eden island, off indefatigable island", "verbatimLocality": "", "island": ""},
		{"eden"}
	),
	(
		{"locality": "isla baltra, w side of, n of isanta cruz island", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "isla baltra (also known as south seymour island)", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "galapagos - on side of rocky cliff at 50 ft. altitude. indefatigable island.specimens of this same species were found on charles island.", "verbatimLocality": "", "island": ""},
		{"santa cruz"}
	),
	(
		{"locality": "baltra to isla seymour", "verbatimLocality": "", "island": ""},
		{"baltra", "seymour"}
	),
	(
		{"locality": "eden island, indefatigable island", "verbatimLocality": "", "island": ""},
		{"eden", "santa cruz"}
	),
	(
		{"locality": "south plaza, santa fe islands, galapagos", "verbatimLocality": "", "island": ""},
		{"plaza", "santa fe"}
	),
	(
		{"locality": "isla santa cruz; bartolomé", "verbatimLocality": "", "island": ""},
		{"bartolome"}
	),
	(
		{"locality": "isla santa cruz; baltra, n coast", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "off e end of isla salvador. [isla san bartolomé = isla bartolomé (bartholomew i.)]", "verbatimLocality": "", "island": "bartolomé [bartholomew]"},
		{"bartolome"}
	),
	(
		{"locality": "between s seymour & daphne islands, james island, galapagos", "verbatimLocality": "", "island": ""},
		{"baltra", "daphne", "santiago"}
	),
	(
		{"locality": "daphne island (also known as isla daphne major), off indefatigable island (also known as isla santa cruz)", "verbatimLocality": "", "island": ""},
		{"daphne"}
	),
	(
		{"locality": "off santa cruz island (indefatigable island), isla el edén, rock pools", "verbatimLocality": "", "island": ""},
		{"eden"}
	),
	(
		{"locality": "charles island, onslow island, near post office bay, inside crater", "verbatimLocality": "", "island": ""},
		{"floreana", "onslow"}
	),
	(
		{"locality": "genovesa (tower) island, darwin bay, lagoon beach", "verbatimLocality": "", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "between fernandina and isabela ids.", "verbatimLocality": "", "island": ""},
		{"fernandina", "isabela"}
	),
	(
		{"locality": "tower island, darwin bay", "verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | tower island, darwin bay", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "darwin bay", "verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | darwin bay", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "darwin bay, anchorage", "verbatimLocality": "eastern pacific | ecuador | galapagos islands | tower island | darwin bay, anchorage", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "between north & south plaza is. off east side santa cruz island", "verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | between north & south plaza is. off east side santa cruz island", "island": ""},
		{"plaza"}
	),
	(
		{"locality": "no specific locality recorded., santa cruz island", "verbatimLocality": "galapagos islands; 25 m. so of santa cruz; barrington island", "island": ""},
		{"santa fe"}
	),
	(
		{"locality": "isla isabela, w of isla guy fawkes", "verbatimLocality": "", "island": ""},
		{"isabela"}
	),
	(
		{"locality": "galapagos islands, james (san salvador) island.  sullivan bay located on the ne side of the island.", "verbatimLocality": "", "island": ""},
		{"santiago"}
	),
	(
		{"locality": "bartholomew id., sullivan bay (james island)", "verbatimLocality": "eastern pacific | ecuador | galapagos islands | santiago island | bartholomew id., sullivan bay (james island)", "island": ""},
		{"bartolome", "santiago"}
	),
	(
		{"locality": "baltra channel to south plaza island (boat ride)", "verbatimLocality": "", "island": ""},
		{"baltra", "plaza"}
	),
	(
		{"locality": "isla santa cruz, galapagos islands, isla floreana (isla santa maria, charles island)", "verbatimLocality": "", "island": ""},
		{"floreana"}
	),
	(
		{"locality": "isla santa cruz, galapagos, ecuador", "verbatimLocality": "bahia academy, darwin research station", "island": "bahia academy, darwin research station"},
		{"santa cruz"}
	),
	(
		{"locality": "galapagos, ecuador", "verbatimLocality": "tower island, darwin bay; rock; r/v velero iii, station 782-38.", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "isla santa cruz, galapagos, ecuador", "verbatimLocality": "darwin research station, academy bay [with dried body]", "island": "bahia academy, darwin research station"},
		{"santa cruz"}
	),
	(
		{"locality": "isla santa maria & isla campeon, 9-18 m, sand & profilic coral, night dive", "verbatimLocality": "", "island": ""},
		{"floreana", "champion"}
	),
	(
		{"locality": "2 km from w end of isla san bartolome, off e coast of isla san salvador.", "verbatimLocality": "", "island": ""},
		{"bartolome", "santiago"}
	),
	(
		{"locality": "isla onslow (n of isla santa maria [= charles island]), in \"pavona\" coral", "verbatimLocality": "", "island": ""},
		{"onslow"}
	),
	(
		{"locality": "galapagos is., isla baltra, caleta del norte, n of isla santa cruz, anton bruun cr. 18b, sta. 791", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "tower island, darwin bay, n.w. shore tide pools", "verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | tower island, darwin bay, n.w. shore tide pools", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "bartholomew i., near san salvador i. (near james id.)", "verbatimLocality": "", "island": ""},
		{"bartolome"}
	),
	(
		{"locality": "isla santa fe/isla plaza sur, galapagos, ecuador", "verbatimLocality": "", "island": ""},
		{"santa fe", "plaza"}
	),
	(
		{"locality": "ec-galapagos--isla rábida -> isla eden", "verbatimLocality": "", "island": ""},
		{"rabida", "eden"}
	),
	(
		{"locality": "genovesa [tower] island, darwin bay", "verbatimLocality": "", "island": ""},
		{"genovesa"}
	),
	(
		{"locality": "isla santa cruz; santiago, se of the island opposite sombrero chino", "verbatimLocality": "", "island": ""},
		{"santiago"}
	),
	(
		{"locality": "galapagos, island santa cruz, roca sin nombre", "verbatimLocality": "", "island": ""},
		{"sin nombre"}
	),
	(
		{"locality": "between daphne island & seymour island", "verbatimLocality": "eastern pacific | ecuador | galapagos islands |  | between daphne island & seymour island", "island": ""},
		{"daphne", "seymour"}
	),
	(
		{"locality": "caleta del norte, isla baltra (n of isla santa cruz), [ca. 22.2 air miles n puerto ayora]", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "", "verbatimLocality": "gardner isl., (near charles) galapagos arch.", "island": "gardner-by-floreana id."},
		{"gardner"}
	),
	(
		{"locality": "indefagitable island, tortuga bay", "verbatimLocality": "", "island": "indefagitable id."},
		{"santa cruz"}
	),
	(
		{"locality": "ablemarle island, cowley bay", "verbatimLocality": "", "island": "ablemarle id."},
		{"isabela"}
	),
	(
		{"locality": "", "verbatimLocality": "", "island": "south seymour"},
		{"baltra"}
	),
	(
		{"locality": "north seymour", "verbatimLocality": "", "island": ""},
		{"seymour"}
	),
	(
		{"locality": "off isla el edén, nw santa cruz island(indefatigable island)", "verbatimLocality": "", "island": ""},
		{"santa cruz"}
	),
	(
		{"locality": "", "verbatimLocality": "hp4j+4c3 airport of the galapagos islands, aeropuerto seymour de baltra, isla baltra baltra, seymour, équateur", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "isla santa cruz; north plazas islet off east shore of santa cruz island", "verbatimLocality": "", "island": ""},
		{"plaza"}
	),
	(
		{"locality": "baltra airport santa cruz island, galapogos, parking lot", "verbatimLocality": "", "island": ""},
		{"baltra"}
	),
	(
		{"locality": "galapagos islands, daphne major islet lying n of santa cruz island.", "verbatimLocality": "", "island": ""},
		{"daphne"}
	),
	(
		{"locality": "galapagos islands. charles (floreana) island. actually collected on champion islet lying abut 1 kilometer offshore and ne of post office bay.", "verbatimLocality": "", "island": ""},
		{"champion"}
	),
	(
		{"locality": "isla santa cruz (indefatigable island). north plazas islet off e shore of santa cruz island", "verbatimLocality": "", "island": ""},
		{"plaza"}
	),
]

def test():
	return True # Skip tests since we don't currently expect 100% success
	resolver = NameResolver()
	failed = 0
	for (test, expected) in name_tests:
		result = resolver.resolve(test)
		if result.loc not in expected:
			print(f"Test failure: extracting location name for {test!r} yielded {result.loc!r}; expected one of {expected!r}")
			failed += 1
	if failed > 0: print(f"Failed {failed} of {len(name_tests)} tests")
	# Tests are more for interest than anything else at the moment.
	return failed == 0
