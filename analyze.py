#! /usr/bin/env python

import logging
import pandas as pd
import sys

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

	# Try resolvers one at a time until one succeeds.  Suspended for now while we test.
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

	def process(self, df):
		tot = len(df)
		for (i, row) in df.iterrows():
			self.rows += 1
			results = self.resolve_consensus(row)
			self.results.append([int(row["gbifID"])] + results)
			print(f"{i}/{tot}: {results!r}")

	def print_stats(self):
		print(f"Overall: {self.rows} rows processed") #, {len(self.resolved)} resolved, {len(self.unresolved)} unresolved")
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			print(f"{name} resolver: {stats.processed} processed, {stats.identified} identified, {stats.excluded} excluded, "
				f"{stats.unknown} unknown, {len(stats.errors)} errors")

	def errors(self):
		for (resolver_id, stats) in enumerate(self.stats):
			name = self.resolvers[resolver_id].name
			for (row, msg) in stats.errors:
				yield (name, msg, row)

def test():
	ok = True
	for mod in [latlon, name]: ok = mod.test() and ok
	if not ok:
		print("Tests failed")
		exit(1)

def main(args):
	logging.basicConfig(level=logging.DEBUG)
	islands.init("galapagos.geojson")
	test()
	data = pd.read_csv(args[1], sep="\t", dtype=str, na_filter=False)
	print(f"Read {len(data)} rows")
	processor = LocationProcessor()
	processor.process(data)
	print("Writing out results...")
	pd.DataFrame(processor.results, columns=["gbifID"] + [ resolver.name for resolver in processor.resolvers ]).to_csv("results.csv", sep="\t", index=False)
	#pd.DataFrame(processor.resolved, columns=["gbifID", "loc"]).to_csv("resolved.csv", sep="\t", index=False)
	#pd.DataFrame(processor.unresolved).to_csv("unresolved.csv", sep="\t", index=False)
	#pd.DataFrame(processor.excluded).to_csv("excluded.csv", sep="\t", index=False)
	with open("errors.txt", "w") as out:
		for (resolver, msg, row) in processor.errors():
			out.write(f"{resolver}: {msg} for row:\n{row}\n\n")
	processor.print_stats()

if __name__ == "__main__": main(sys.argv)
