#! /usr/bin/env python

import logging
import pandas as pd
import sys

import islands
import latlon
import name
import processor

def test():
	ok = True
	for mod in [latlon, name]: ok = mod.test() and ok
	if not ok:
		print("Tests failed")
		exit(1)

def main(args):
	logging.basicConfig(level=logging.DEBUG)
	test()
	islands.init("data/galapagos.geojson")
	data = pd.read_csv(args[1], sep="\t", quoting=3, dtype=str, na_filter=False)
	print(f"Read {len(data)} rows")
	resolver = processor.LocationProcessor()
	resolver.process(data)
	print("Writing out results...")
	pd.DataFrame(resolver.results, columns=["gbifID"] + [ resolver.name for resolver in resolver.resolvers ]).to_csv("results.csv", sep="\t", index=False)
	#pd.DataFrame(resolver.resolved, columns=["gbifID", "loc"]).to_csv("resolved.csv", sep="\t", index=False)
	#pd.DataFrame(resolver.unresolved).to_csv("unresolved.csv", sep="\t", index=False)
	#pd.DataFrame(resolver.excluded).to_csv("excluded.csv", sep="\t", index=False)
	with open("errors.txt", "w") as out:
		for (resolver, msg, row) in resolver.errors():
			out.write(f"{resolver}: {msg} for row:\n{row}\n\n")
	resolver.print_stats()

if __name__ == "__main__": main(sys.argv)
