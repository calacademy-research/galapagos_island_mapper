from base import *
import latlon
import name

RESOLVERS = [
	latlon.LatLonResolver,
	name.NameResolver,
]

class ResolverStat:
	def __init__(self, name):
		self.name = name
		self.processed = 0
		self.identified = 0
		self.unknown = 0
		self.errors = []
		self.agreements = 0
		self.soft_disagreements = 0
		self.hard_disagreements = 0

	def print(self):
		print(f"{self.name} resolver: {self.processed} processed, {self.identified} identified, "
			f"{self.unknown} unknown, {len(self.errors)} errors, {self.agreements} agree, "
			f"{self.hard_disagreements} hard/{self.soft_disagreements} soft disagree")

	@staticmethod
	def create():
		return { res.name: ResolverStat(res.name) for res in RESOLVERS }

class LocationProcessor:
	"""Resolve observations to islands.

	This class performs the basic job of taking a GBIF data set and returning a mapping of GBIF IDs to a set of resolutions, each of
	which is basically a statement that we believe that the observation took place on that island with some degree of confidence.
	More abstract analysis, such as deciding which reolution is the best or counting species per island, should happen elsewhere.
	"""

	def __init__(self):
		self.resolvers = [ resolver() for resolver in RESOLVERS ]

	def resolve(self, row, stats):
		results = []
		for resolver in self.resolvers:
			stat = stats[resolver.name]
			stat.processed += 1
			try:
				res = resolver.resolve(row)
				results.extend(res)
				if res == []: stat.unknown += 1
				else: stat.identified += 1
			except Exception as e: stat.errors.append((row, str(e)))
		return results

class Prioritizer:
	"""Prioritize and choose location resolutions.

	Given a GBIF row and a set of resolutions, this class applies a policy to select one or more "best" resolutions to return.  For
	example, coordinates are much less likely to be the primary observation before GPS was invented, so we deprioritize coordinate-
	based resolutions before 1980.
	"""

	conf_val = {LOW: 0, MODERATE: 1, HIGH: 2}
	resolver_names = [ resolver.name for resolver in RESOLVERS ]

	def best_resolution(self, resolutions):
		if len(resolutions) == 0: return UNKNOWN
		ret = resolutions[0]
		for res in resolutions[1:]:
			if self.conf_val[res.conf] > self.conf_val[ret.conf]: ret = res
		return ret

	def best_by_resolver(self, resolutions):
		ret = {}
		for res in resolutions:
			if res.resolver not in ret: ret[res.resolver] = res
			else:
				existing = ret[res.resolver]
				if self.conf_val[res.conf] > self.conf_val[existing.conf]: ret[res.resolver] = res
		return ret

	def choose(self, row, resolutions, stats):
		if len(resolutions) == 0: return UNKNOWN
		if len(resolutions) == 1:
			stats[resolutions[0].resolver].agreements += 1
			return resolutions[0]

		# Do some accounting -- yes, this duplicates some of `best_by_resolver`.
		all_by_resolver = {}
		best_by_resolver = {}
		island_resolvers = {}
		for res in resolutions:
			all_by_resolver.setdefault(res.resolver, []).append(res)
			if res.loc is not None: island_resolvers.setdefault(res.loc, set()).add(res.resolver)
		for (resolver, res) in all_by_resolver.items():
			best_by_resolver[resolver] = self.best_resolution(res)
		ret = None

		# If one island was chosen by all resolvers, immediately accept it.
		islands_chosen_by_all_resolvers = set()
		for (island, resolvers) in island_resolvers.items():
			if resolvers == set(self.resolver_names): islands_chosen_by_all_resolvers.add(island)
		if len(islands_chosen_by_all_resolvers) == 1:
			choice = islands_chosen_by_all_resolvers.pop()
			ret = self.best_resolution([ res for res in resolutions if res.loc == choice ])

		# Otherwise, return the highest-confidence resolution available.
		if ret is None:
			# Special case: points near Española may belong to Isla Gardner de Española and be mislabeled as belonging to Isla Gardner de
			# Floreana.  We consider this island to be part of Española.
			if best_by_resolver.get("latlon", UNKNOWN).loc == "espanola" and best_by_resolver.get("name", UNKNOWN).loc == "gardner":
				ret = best_by_resolver["latlon"]
			# Before GPS was widely available, lat/lon are much more likely to be derived values.
			elif row["year"] != "" and int(row["year"]) < 1980 and "name" in best_by_resolver:
				ret = best_by_resolver["name"]
			# iNaturalist is expected to have good GPS-based lat/lon values.
			elif row["publisher"] == "iNaturalist.org" and "latlon" in best_by_resolver:
				ret = best_by_resolver["latlon"]

		if ret is None: ret = self.best_resolution(resolutions)

		# Update statistics and return
		for resolver in self.resolver_names:
			if resolver in all_by_resolver:
				stat = stats[resolver]
				if ret.loc not in { res.loc for res in all_by_resolver[resolver] }:
					stat.hard_disagreements += 1
				if ret.loc != best_by_resolver[resolver].loc:
					stat.soft_disagreements += 1
				else:
					stat.agreements += 1
		return ret
