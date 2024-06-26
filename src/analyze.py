#! /usr/bin/env python

import configparser
import datetime
import logging
import os.path
import pandas
import sys

from base import *
import islands
import latlon
import name
import process
import taxonomy

def test():
	ok = True
	for mod in [latlon, name]: ok = mod.test() and ok
	if not ok:
		print("Tests failed")
		exit(1)

def main(args):
	# Setup
	starttime = datetime.datetime.now()
	print("Loading config")
	logging.basicConfig(level=logging.WARNING)
	config = configparser.ConfigParser()
	conffile = "config.ini"
	if not os.path.isfile(conffile): raise RuntimeError(f"Can't open configuration file {conffile!r}")
	config.read(conffile)
	test()
	islands.init(config.get("input", "geometry"))
	stats = process.ResolverStat.create()
	resolver = process.LocationProcessor()
	chooser = process.Prioritizer()
	mapper = taxonomy.ObservationMapper(config.get("input", "taxonomy"))

	# Read and process data
	print("Reading GBIF")
	datafile = args[1] if len(args) > 1 else config.get("input", "gbif")
	data = {}
	for (_, row) in pandas.read_csv(datafile, sep="\t", quoting=3, dtype=str, na_filter=False).iterrows():
		data[row["gbifID"]] = row
	tot = len(data)
	processed = 0
	resolved = 0
	skipped = 0
	results = []
	print(f"Read {len(data)} rows from {datafile}")
	for row in data.values():
		processed += 1
		#if not mapper.should_include(row):
		#	skipped += 1
		#	continue
		res = resolver.resolve(row, stats)
		best = chooser.choose(row, res, stats)
		if best != UNKNOWN: resolved += 1
		best_by_resolver = chooser.best_by_resolver(res)
		name_best = best_by_resolver.get("name", UNKNOWN).loc or "-"
		latlon_best = best_by_resolver.get("latlon", UNKNOWN).loc or "-"
		if best.loc is not None: mapper.add(row, best.loc)
		results.append([int(row["gbifID"]), name_best, latlon_best, best.loc or "-", taxonomy.most_specific_taxon(row) or "-"])
		if processed % 100 == 0: print(f"\r{processed}/{tot}", end="")
	print()

	# Write results
	print("Writing out results")
	#results = [ { "gbifID": k, "resolutions": [ r.fields() for r in v ] } for (k, v) in resolver.results.items() ] # JSON
	header = ["gbifID", "name", "latlon", "best", "species"]
	pandas.DataFrame(results, columns=header).to_csv(config.get("output", "results"), sep="\t", index=False)
	with open(config.get("output", "errors"), "w") as out:
		for stat in stats.values():
			for (row, msg) in stat.errors:
				out.write(f"{stat.name}: {msg} for row:\n{row}\n\n")
	print(f"Overall: {processed} rows processed, {resolved} resolved, {skipped} skipped")
	for stat in stats.values(): stat.print()
	mapper.summarize().to_tsv(config.get("output", "observations"))
	duration = (datetime.datetime.now() - starttime).total_seconds()
	print(f"Entire run took {int(duration / 60)} minutes, {int(duration % 60)} seconds")

if __name__ == "__main__": main(sys.argv)
