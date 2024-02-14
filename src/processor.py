from base import *
import islands
import latlon
import name

class LocationProcessor:
	class ResolverStat:
		def __init__(self):
			self.processed = 0
			self.identified = 0
			self.unknown = 0
			self.excluded = 0
			self.errors = []

	def __init__(self):
		self.resolvers = [
			latlon.LatLonResolver(),
			name.NameResolver(),
		]
		self.stats = [ self.ResolverStat() for _ in self.resolvers ]
		self.rows = 0
		self.results = []
		self.species = {}
		self.observations = {}

	# Try resolvers one at a time until one succeeds.
	def resolve_cascading(self, row):
		for (resolver_id, resolver) in enumerate(self.resolvers):
			stats = self.stats[resolver_id]
			stats.processed += 1
			try: res = resolver.resolve(row)
			except Exception as e:
				stats.errors.append((row, str(e)))
				continue
			if res.loc is None and res.conf == HIGH:
				stats.excluded += 1
				return res
			if res.loc is None:
				stats.unknown += 1
				continue
			normal = name.normalize(res.loc)
			if normal not in islands.names:
				stats.errors.append((row, f"Returned invalid name {res.loc!r}"))
				continue
			stats.identified += 1
			res.loc = normal
			return res
		return UNKNOWN

	# Query all resolvers for each point and see whether they agree with each other.
	def resolve_consensus(self, row):
		results = []
		for (resolver_id, resolver) in enumerate(self.resolvers):
			stats = self.stats[resolver_id]
			stats.processed += 1
			try: res = resolver.resolve(row)
			except Exception as e:
				stats.errors.append((row, str(e)))
				results.append("-")
				continue
			if res.loc is None and res.conf == HIGH:
				stats.excluded += 1
				results.append("-")
			elif res.loc is None:
				stats.unknown += 1
				results.append("-")
			else:
				normal = name.normalize(res.loc)
				if normal not in islands.names:
					stats.errors.append((row, f"Returned invalid name {res.loc!r}"))
					results.append("-")
				else:
					stats.identified += 1
					results.append(normal)
		return results

	# Use the highest-confidence resolution after applying heuristics about their likely trustworthiness
	def resolve_priority(self, row):
		raise NotImplementedError()

	def process(self, df):
		tot = len(df)
		for (i, row) in df.iterrows():
			self.rows += 1
			results = self.resolve_consensus(row)
			self.results.append([int(row["gbifID"])] + results)
			species = row["species"] or row["genus"]
			for island in results:
				if island == "-": continue
				k = (species, island)
				self.observations.setdefault(k, 0)
				self.observations[k] += 1
				self.species.setdefault(species, 0)
				self.species[species] += 1
			if i % 100 == 0: print(f"\r{i}/{tot}", end="")
		print()

	def print_stats(self):
		print(f"Overall: {self.rows} rows processed") #, {len(self.resolved)} resolved, {len(self.unresolved)} unresolved")
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			print(f"{name} resolver: {stats.processed} processed, {stats.identified} identified, {stats.excluded} excluded, "
				f"{stats.unknown} unknown, {len(stats.errors)} errors")

	def print_observations(self):
		island_names = sorted(islands.names)
		top_species = sorted((spec for (spec, count) in self.species.items() if count >= 100), reverse=True)
		print("," + ",".join(island_names))
		for species in top_species:
			print(species, end="")
			for island in island_names:
				print("," + str(self.observations.get((species, island), 0)), end="")
			print()

	def errors(self):
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			for (row, msg) in stats.errors:
				yield (name, msg, row)
