import dataclasses
import logging
import osgeo.ogr
import shapely

from base import *

@dataclasses.dataclass
class Island:
	name: str
	osmids: list[int]
	aliases: set[str]
	geometry: list[shapely.Polygon]

	def __hash__(self): return hash(self.name)

islands = [
	Island(
		name = "baltra",
		osmids = [2129829],
		aliases = {"seymour"},
		geometry = [],
	),
	Island(
		name = "bartolome",
		osmids = [13299590],
		aliases = {"bartholomew"},
		geometry = [],
	),
	Island(
		name = "beagle",
		osmids = [13402845, 13402844],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "caldwell",
		osmids = [5113389],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "champion",
		osmids = [34201438],
		aliases = {"campeon"},
		geometry = [],
	),
	Island(
		name = "cowley",
		osmids = [5113851],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "crossman",
		osmids = [5113483, 5113475, 146294607, 5113481, 6171480, 5113476],
		aliases = {"cuatro hermanos"},
		geometry = [],
	),
	Island(
		name = "daphne",
		osmids = [5113815, 5113846],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "darwin",
		osmids = [551730596, 551727784, 551727777, 551727780, 551727776],
		aliases = {"culpepper"},
		geometry = [],
	),
	Island(
		name = "eden",
		osmids = [5113629],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "enderby",
		osmids = [34201518],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "espanola",
		# FIXME This relation is not a closed way, and is currently missing from the data.
		osmids = [34159403],
		aliases = {"hood"},
		geometry = [],
	),
	Island(
		name = "fernandina",
		osmids = [2130001],
		aliases = {"marborough"},
		geometry = [],
	),
	Island(
		name = "floreana",
		osmids = [2566632],
		aliases = {"charles", "santa maria"},
		geometry = [],
	),
	Island(
		name = "gardner",
		osmids = [5113388],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "genovesa",
		osmids = [5114780],
		aliases = {"tower"},
		geometry = [],
	),
	Island(
		name = "guy fawkes",
		osmids = [5113651, 5113654],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "isabela",
		osmids = [2129921],
		aliases = {"albemarle"},
		geometry = [],
	),
	Island(
		name = "marchena",
		osmids = [13399789],
		aliases = {"bindloe"},
		geometry = [],
	),
	Island(
		name = "onslow",
		osmids = [34201564],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "pinta",
		osmids = [4538042],
		aliases = {"abingdon"},
		geometry = [],
	),
	Island(
		name = "pinzon",
		osmids = [303268103],
		aliases = {"duncan"},
		geometry = [],
	),
	Island(
		name = "plaza",
		osmids = [5113617, 5113616],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "rabida",
		osmids = [13299861],
		aliases = {"jervis"},
		geometry = [],
	),
	Island(
		name = "san cristobal",
		osmids = [2128941],
		aliases = {"chatham"},
		geometry = [],
	),
	Island(
		name = "santa cruz",
		osmids = [2129845],
		aliases = {"indefatigable"},
		geometry = [],
	),
	Island(
		name = "santa fe",
		osmids = [4538087],
		aliases = {"barrington"},
		geometry = [],
	),
	Island(
		name = "santiago",
		osmids = [2129890],
		aliases = {"san salvador", "james"},
		geometry = [],
	),
	Island(
		name = "seymour",
		osmids = [5113849],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "sin nombre",
		osmids = [5113576],
		aliases = {"nameless"},
		geometry = [],
	),
	Island(
		name = "tortuga",
		osmids = [5194328],
		aliases = {"brattle"},
		geometry = [],
	),
	Island(
		name = "watson",
		osmids = [5113383],
		aliases = set(),
		geometry = [],
	),
	Island(
		name = "wolf",
		osmids = [551724900, 551724984, 551724959, 551724964, 551724955],
		aliases = {"wenman"},
		geometry = [],
	),
]

names = { island.name for island in islands }

def recursive_yield_points(geo):
	for subgeo in range(geo.GetGeometryCount()):
		for subsubgeo in recursive_yield_points(geo.GetGeometryRef(subgeo)):
			yield subsubgeo
	if geo.GetPointCount() > 0:
		# Convert from (x, y, z) to (lat, lon)
		yield [ (geo.GetPoint(i)[1], geo.GetPoint(i)[0]) for i in range(geo.GetPointCount()) ]

def init(osm_path):
	logging.info("Loading island geometries")
	osgeo.ogr.UseExceptions()
	f = osgeo.ogr.Open(osm_path)
	polygons = f.GetLayerByName("multipolygons")
	for island in islands:
		for osmid in island.osmids:
			polygons.SetAttributeFilter(f"osm_id = '{osmid}' or osm_way_id = '{osmid}'")
			feats = [ feat for feat in polygons ]
			assert(len(feats) <= 1)
			if len(feats) == 1: feat = feats[0]
			else:
				logging.warning(f"Missing geometry for OSM feature {osmid}.  Island assignments may be inaccurate.")
				continue
			for poly in recursive_yield_points(feat.GetGeometryRef()):
				if len(poly) <= 2: continue
				if poly[-1] != poly[0]: poly.append(poly[0])
				island.geometry.append(shapely.Polygon(poly))
		#logging.info(f"Read {len(island.geometry)} polygons for {island.name}")
