from base import *
import latlon
import name

class LocationProcessor:
	"""Resolve observations to islands.

	This class performs the basic job of taking a GBIF data set and returning a mapping of GBIF IDs to a set of resolutions, each of
	which is basically a statement that we believe that the observation took place on that island with some degree of confidence.
	More abstract analysis, such as deciding which reolution is the best or counting species per island, should happen elsewhere.
	"""

	class ResolverStat:
		def __init__(self):
			self.processed = 0
			self.identified = 0
			self.unknown = 0
			self.errors = []

	def __init__(self):
		self.resolvers = [
			latlon.LatLonResolver(),
			name.NameResolver(),
		]
		self.stats = [ self.ResolverStat() for _ in self.resolvers ]
		self.rows = 0
		self.results = {}
		self.observations = {}

	def resolve(self, row):
		results = []
		for (resolver_id, resolver) in enumerate(self.resolvers):
			stats = self.stats[resolver_id]
			stats.processed += 1
			try:
				res = resolver.resolve(row)
				results.extend(res)
				if res == []: stats.unknown += 1
				else: stats.identified += 1
			except Exception as e: stats.errors.append((row, str(e)))
		return results

	def process(self, rows):
		tot = len(rows)
		for (i, row) in enumerate(rows):
			self.rows += 1
			results = self.resolve(row)
			self.results[int(row["gbifID"])] = results
			if i % 100 == 0: print(f"\r{i}/{tot}", end="")
		print()

	def print_stats(self):
		print(f"Overall: {self.rows} rows processed")
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			print(f"{name} resolver: {stats.processed} processed, {stats.identified} identified, "
				f"{stats.unknown} unknown, {len(stats.errors)} errors")

	def errors(self):
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			for (row, msg) in stats.errors:
				yield (name, msg, row)
