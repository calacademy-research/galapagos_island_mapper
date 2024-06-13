import dataclasses
import json
import logging
import numbers
import shapely

from base import *

@dataclasses.dataclass
class Island:
	name: str
	osmids: list[int]
	aliases: set[str] = dataclasses.field(default_factory=set)
	geometry: list[shapely.Polygon] = dataclasses.field(default_factory=list)

	def __hash__(self): return hash(self.name)

islands = [
	Island(
		name = "baltra",
		osmids = [2129829],
		aliases = {"south seymour", "s seymour"},
	),
	Island(
		name = "bartolome",
		osmids = [13299590],
		aliases = {"bartholomew"},
	),
	Island(
		name = "beagle",
		osmids = [13402845, 13402844],
	),
	Island(
		name = "caldwell",
		osmids = [5113389],
	),
	Island(
		name = "champion",
		osmids = [34201438],
		aliases = {"campeon", "campion"},
	),
	Island(
		name = "cowley",
		osmids = [5113851],
	),
	Island(
		name = "crossman",
		osmids = [5113483, 5113475, 146294607, 5113481, 6171480, 5113476],
		aliases = {"cuatro hermanos"},
	),
	Island(
		name = "daphne",
		osmids = [5113815, 5113846],
	),
	Island(
		name = "darwin",
		osmids = [551730596, 551727784, 551727777, 551727780, 551727776],
		aliases = {"culpepper"},
	),
	Island(
		name = "eden",
		osmids = [5113629],
		aliases = {"el eden"}
	),
	Island(
		name = "enderby",
		osmids = [34201518],
	),
	Island(
		name = "espanola",
		# Warning!  Order matters in this list, because this island coastline is built from a series of fragments that must be in order.
		osmids = [992208855, 34159403, 992137192, 992137189, 992137188, 992208859, 992208854, 992208856, 34159728],
		aliases = {"hood"},
	),
	Island(
		name = "fernandina",
		osmids = [2130001],
		aliases = {"narborough"},
	),
	Island(
		name = "floreana",
		osmids = [2566632],
		aliases = {"charles", "santa maria"},
	),
	Island(
		# NOTE: There are two Gardner Islands in the Galapagos: one just east of Floreana and one just north of Española.
		# Unfortunately, there is no standard way of distinguishing these from each other.  Often you will see one of the nearby islands
		# mentioned in conjunction with Gardner to disambiguate, but this is not easy to pull out algorithmically.  The lat/lon resolver
		# will only assign points located near Gardner de Floreana to "Gardner", but the name-based resolver will also make this
		# assignment for points mentioning Gardner de Española.  Currently, we deal with this half-heartedly by deprioritizing name-
		# based resolutions for islands that the lat/lon resolver places near Española.
		name = "gardner",
		osmids = [5113388],
	),
	Island(
		name = "genovesa",
		osmids = [5114780],
		aliases = {"tower"},
	),
	Island(
		name = "guy fawkes",
		osmids = [5113651, 5113654],
	),
	Island(
		name = "isabela",
		osmids = [2129921],
		aliases = {"albemarle", "ablemarle"},
	),
	Island(
		name = "marchena",
		osmids = [13399789],
		aliases = {"bindloe"},
	),
	Island(
		name = "onslow",
		osmids = [34201564],
	),
	Island(
		name = "pinta",
		osmids = [4538042],
		aliases = {"abingdon"},
	),
	Island(
		name = "pinzon",
		osmids = [303268103],
		aliases = {"duncan"},
	),
	Island(
		name = "plaza",
		osmids = [5113617, 5113616],
	),
	Island(
		name = "rabida",
		osmids = [13299861],
		aliases = {"jervis"},
	),
	Island(
		name = "san cristobal",
		osmids = [2128941],
		aliases = {"chatham"},
	),
	Island(
		name = "santa cruz",
		osmids = [2129845],
		aliases = {"indefatigable", "indefagitable", "puerto ayora"},
	),
	Island(
		name = "santa fe",
		osmids = [4538087],
		aliases = {"barrington"},
	),
	Island(
		name = "santiago",
		osmids = [2129890],
		aliases = {"san salvador", "james", "sombrero chino"},
	),
	Island(
		name = "seymour",
		osmids = [5113849],
	),
	Island(
		name = "sin nombre",
		osmids = [5113576],
		aliases = {"nameless"},
	),
	Island(
		name = "tortuga",
		osmids = [5194328],
		aliases = {"brattle"},
	),
	Island(
		name = "watson",
		osmids = [5113383],
	),
	Island(
		name = "wolf",
		osmids = [551724900, 551724984, 551724959, 551724964, 551724955],
		aliases = {"wenman"},
	),
]

names = { island.name for island in islands }

class PolygonAccumulator:
	def __init__(self):
		self.finished = []
		self.cur = []

	def _finish(self, poly):
		if len(poly) <= 2: return
		if poly[-1] != poly[0]: poly.append(poly[0])
		elif len(poly) <= 3: return
		self.finished.append(shapely.Polygon(poly))

	def _finish_cur(self):
		self._finish(self.cur)
		self.cur = []
	
	def add(self, poly):
		if len(poly) <= 1: return
		if poly[-1] == poly[0]:
			self._finish_cur()
			self._finish(poly)
			return
		if self.cur != []:
			if poly[0] == self.cur[-1]:
				self.cur.extend(poly)
				if self.cur[-1] == self.cur[0]: self._finish_cur()
			else:
				self._finish_cur()
				self.cur = poly
		else: self.cur = poly

	def retrieve(self):
		self._finish_cur()
		return self.finished

def recursive_yield_polygons(geo):
	if not isinstance(geo, list): raise RuntimeError("Unexpected non-list when processing geometries")
	if geo == []: return
	ret = []
	for subgeo in geo:
		if len(subgeo) == 2 and isinstance(subgeo[0], numbers.Number) and isinstance(subgeo[1], numbers.Number):
			ret.append((subgeo[1], subgeo[0]))
		else:
			for poly in recursive_yield_polygons(subgeo): yield poly
	if ret != []: yield ret

def init(osm_path):
	logging.info("Loading island geometries")
	with open(osm_path) as f: island_data = json.load(f)
	polygons = {}
	for feature in island_data["features"]:
		osmid = int(feature["properties"].get("osm_id") or feature["properties"].get("osm_way_id"))
		polygons[osmid] = recursive_yield_polygons(feature["geometry"]["coordinates"])
	for island in islands:
		accumulator = PolygonAccumulator()
		for osmid in island.osmids:
			if osmid not in polygons:
				logging.warning(f"Missing geometry for OSM feature {osmid}.  Island assignments may be inaccurate.")
				continue
			for poly in polygons[osmid]: accumulator.add(poly)
		island.geometry.extend(accumulator.retrieve())
		logging.info(f"Built {len(island.geometry)} polygons from {len(island.osmids)} OSM ways for {island.name}")
