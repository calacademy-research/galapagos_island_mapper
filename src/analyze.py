#! /usr/bin/env python

import configparser
import csv
import json
import logging
import os.path
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
	conffile = "config.ini"
	if not os.path.isfile(conffile): raise RuntimeError(f"Can't open configuration file {conffile!r}")
	config.read(conffile)
	test()
	islands.init(config.get("files", "geometry"))
	datafile = args[1] if len(args) > 1 else config.get("files", "gbif")
	with open(datafile) as inf: data = list(csv.DictReader(inf, delimiter="\t"))
	print(f"Read {len(data)} input rows from f{datafile}")
	resolver = processor.LocationProcessor()
	resolver.process(data)
	print("Writing out results...")
	results = [ { "gbifID": k, "resolutions": [ r.fields() for r in v ] } for (k, v) in resolver.results.items() ]
	with open(config.get("files", "results"), "w") as outf: json.dump(results, outf)
	with open("errors.txt", "w") as out:
		for (name, msg, row) in resolver.errors():
			out.write(f"{name}: {msg} for row:\n{row}\n\n")
	resolver.print_stats()

if __name__ == "__main__": main(sys.argv)
