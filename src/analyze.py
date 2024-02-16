#! /usr/bin/env python

import configparser
import logging
import os.path
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
	config = configparser.ConfigParser()
	conffile = args[1] if len(args) > 1 else "config.ini"
	if not os.path.isfile(conffile): raise RuntimeError(f"Can't open configuration file {conffile!r}")
	config.read(conffile)
	test()
	islands.init(config.get("sources", "geometry"))
	data = pd.read_csv(config.get("sources", "gbif"), sep="\t", quoting=3, dtype=str, na_filter=False)
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
