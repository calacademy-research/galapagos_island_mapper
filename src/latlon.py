import functools
import parsimonious.grammar
import parsimonious.nodes
import re
import shapely

from base import *
import islands

class LatLonResolver(Resolver):
	"""Resolve observations to island names based on latitude and longitude.

	Aside from the process of parsing coordinates, this resolver is very cut-and-dried.  We apply a margin of 0.02 degrees around
	each island, and any point falling within the island or that margin is treated as occurring on the island.  If a point is in the
	intersection of two margins, we return both islands.
	"""

	name = "latlon"
	precision = 3
	min = (-1.70, -92.30)
	max = (1.90, -89.00)

	class BufferedMultiPolygon:
		margin = 0.02 # Ascribe to a given island anything within 0.02 degrees of it -- about one mile in this region.

		def __init__(self):
			self.ground = None
			self.buffer = None

		def add(self, poly):
			buffered = poly.buffer(self.margin)
			if self.ground is None:
				self.ground = poly
				self.buffer = buffered
			else:
				self.ground = shapely.union(self.ground, poly)
				self.buffer = shapely.union(self.buffer, buffered)

	class CoordVisitor(parsimonious.nodes.NodeVisitor):
		def generic_visit(self, node, children): return children or node

		def visit_num(self, node, children):
			if node.text == "--": return 0.0
			return float(node.text.replace(",", "."))

		def visit_dir(self, node, children): return node.text[0].lower()

		def visit_deg(self, node, children): return children[0]

		def visit_min(self, node, children): return children[0] / 60.0

		def visit_sec(self, node, children): return children[0] / 3600.0

		def visit_degminsec_std(self, node, children):
			def maybe(part):
				if isinstance(part, list): return part[0]
				if isinstance(part, float): return part
				return 0.0
			if isinstance(children[2], list): dir = children[2][0]
			else: dir = None
			parts = children[0][0]
			if len(parts) == 1: val = parts[0]
			elif len(parts) == 3: val = parts[0] + maybe(parts[2])
			elif len(parts) == 5: val = parts[0] + maybe(parts[2]) + maybe(parts[4])
			else: raise RuntimeError("Didn't hit expected case in degminsec_std")
			return (val, dir)

		def visit_degminsec_pre(self, node, children):
			maybe = lambda part: part[0] if isinstance(part, list) else 0.0
			return (children[1] + maybe(children[3]), children[0][0])

		def visit_degminsec_unmarked(self, node, children):
			maybe = lambda part: part[0] if isinstance(part, list) else 0.0
			return (children[0] + maybe(children[2]) / 60.0 + maybe(children[4]) / 3600.0, children[6])

		def visit_degminsec_merged(self, node, children):
			def int_dashes(s):
				if re.search("^-+$", s): return 0
				return int(s)
			val = children[0].text
			(deg, min, sec) = ("0", "0", "0")
			if len(val) <= 3: deg = val
			elif len(val) <= 5: (deg, min) = (val[:-2], val[-2:])
			else: (deg, min, sec) = (val[:-4], val[-4:-2], val[-2:])
			(deg, min, sec) = (int_dashes(deg), int_dashes(min), int_dashes(sec))
			return (deg + min / 60 + sec / 3600, children[2])

		def visit_degminsec(self, node, children): return children[2][0]

		def visit_enclosed_latlon(self, node, children): return children[1]

		def visit_latlon(self, node, children): return (children[0][0][0], children[0][0][4])

	def __init__(self):
		self.coord_grammar = parsimonious.grammar.Grammar(r"""
			ws = ~"\s*"
			whole = ~"\d+"
			decimal = ~"[\.,]\d+"
			num = ("-"? ((whole decimal) / whole / decimal)) / "--"

			degmark = "º" / "°" / ~"deg"i / "d" / "D"
			minmark = "'" / "’" / "′" / "`" / "m" / "M"
			secmark = "\"" / "”" / "''" / "'" / "s" / "S"
			dir = ~"[nsew]\.?"i

			deg = num ws degmark
			min = num ws minmark
			sec = num ws secmark
			degminsec_std = ((deg ws min ws sec) / (deg ws min ws num) / (deg ws min? ws sec?) / (min ws sec?) / sec / (num ws min? ws sec?)) ws dir?
			degminsec_pre = dir deg ws min?
			degminsec_unmarked = num (":" / ws) num? (":" / ws) num? (":" / ws) dir
			degminsec_merged = ~"[0-9-]{1,7}" ws dir
			degminsec = ~"ca\.?"i? ws (degminsec_pre / degminsec_merged / degminsec_unmarked / degminsec_std)

			plain_latlon = degminsec ws ("," / "/" / ";" / ~"\s+")? ws degminsec
			enclosed_latlon = ("(" plain_latlon ")")
			latlon = (enclosed_latlon / plain_latlon) ws
		""")
		self.coord_visitor = self.CoordVisitor()

		self.polygons = {}
		for island in islands.islands:
			for poly in island.geometry:
				if island.name not in self.polygons: self.polygons[island.name] = self.BufferedMultiPolygon()
				self.polygons[island.name].add(poly)

	# Parse a single coordinate, latitude or longitude
	def parse_human_coord(self, s, acceptable_dirs, max_abs):
		(val, dir) = self.coord_visitor.visit(self.coord_grammar["degminsec"].parse(s))
		if dir is not None:
			if dir not in acceptable_dirs: raise RuntimeError(f"Invalid direction {dir!r}")
			if val < 0: raise ValueError(f"Negative coordinate {val} given with direction {dir}")
			if dir in {'s', 'w'}: val = -val
		if val > max_abs or val < -max_abs: raise ValueError(f"Coordinate {val} outside of designated bound {max_abs}")
		return val

	def parse_human_lat(self, s): return self.parse_human_coord(s, {'n', 's'}, 90)

	def parse_human_lon(self, s): return self.parse_human_coord(s, {'e', 'w'}, 180)

	def parse_human_latlon(self, s):
		((lat, latdir), (lon, londir)) = self.coord_visitor.visit(self.coord_grammar["latlon"].parse(s))
		if latdir in {'e', 'w'} and londir in {'n', 's'}:
			(lat, lon) = (lon, lat)
			(latdir, londir) = (londir, latdir)
		if latdir is not None:
			if latdir not in {'n', 's'}: raise RuntimeError(f"Invalid latitude direction {latdir!r}")
			if lat < 0: raise ValueError(f"Negative latitude {lat} given with direction {latdir}")
			if latdir == 's': lat = -lat
		if londir is not None:
			if londir not in {'e', 'w'}: raise RuntimeError(f"Invalid longitude direction {londir!r}")
			if lon < 0: raise ValueError(f"Negative longitude {lon} given with direction {londir}")
			if londir == 'w': lon = -lon
		if lat < -90 or lat > 90: raise ValueError(f"Latitude {lat} out of bounds")
		if lon < -180 or lon > 180: raise ValueError(f"Longitude {lon} out of bounds")
		return (lat, lon)

	def find_coordinates(self, row):
		has_col = lambda name: name in row and row[name] != ""
		if has_col("decimalLatitude") and has_col("decimalLongitude"):
			try: return (float(row["decimalLatitude"]), float(row["decimalLongitude"]))
			except: pass
		if has_col("verbatimLatitude") and has_col("verbatimLongitude"):
			try: return (self.parse_human_lat(row["verbatimLatitude"]), self.parse_human_lon(row["verbatimLongitude"]))
			except: pass
			# If the lat/lon don't parse as-is but do parse when swapped, then it's quite likely they were entered the wrong way around.
			try: return (self.parse_human_lat(row["verbatimLongitude"]), self.parse_human_lon(row["verbatimLatitude"]))
			except: pass
		if has_col("verbatimCoordinates"):
			try: return self.parse_human_latlon(row["verbatimCoordinates"])
			except: pass
		return None

	@functools.cache
	def query(self, lat, lon):
		point = shapely.Point(lat, lon)
		candidates = set()
		for (name, poly) in self.polygons.items():
			if poly.ground.contains(point): return [Resolution(name, HIGH, self.name)]
			if poly.buffer.contains(point): candidates.add(name)
		if len(candidates) == 0: return [Resolution(None, HIGH, self.name)]
		return [ Resolution(cand, MODERATE, self.name) for cand in candidates ]

		# Alternately, we could return the island that the point is closest to.
		#(min, argmin) = (0, None)
		#for candidate in candidates:
		#	dist = shapely.distance(point, self.polygons[candidate].ground)
		#	if argmin is None or dist < min: (min, argmin) = (dist, candidate)
		#return argmin

	def resolve(self, row):
		coords = self.find_coordinates(row)
		if coords is None: return []
		(lat, lon) = coords
		if (
			lat < self.min[0] or
			lon < self.min[1] or
			lat > self.max[0] or
			lon > self.max[1]
		): return [Resolution(None, HIGH, self.name)]
		return self.query(round(lat, self.precision), round(lon, self.precision))

latlon_tests = [
	('s1°39′ w89°20′', (-1.65, -89.33333333333333)),
	('13\' 45" s, 91° 48\' 30" w', (-0.22916666666666669, -91.80833333333334)),
	("0° 44' 29.16'' s 90° 18' 27.56'' w", (-0.7414333333333333, -90.30765555555556)),
	("0° 44' 46.08'' s 90° 17' 59'' w", (-0.7461333333333333, -90.29972222222221)),
	("0° 58' 40'' s 91° 26' 3.47'' w", (-0.9777777777777777, -91.43429722222223)),
	('0,6262°s 90,3863°w', (-0.6262, -90.3863)),
	('0,6377°s 90,3829°w', (-0.6377, -90.3829)),
	('0,693463°s 90,325073°w', (-0.693463, -90.325073)),
	('0,2743°s 90,7148°w', (-0.2743, -90.7148)),
	('-.81639/-90.05', (-0.81639, -90.05)),
	('-1.23306/-90.44972', (-1.23306, -90.44972)),
	('-.75/-90.28306', (-0.75, -90.28306)),
	('-1.25218/-90.46932', (-1.25218, -90.46932)),
	('0/-90', (0.0, -90.0)),
	('-.4/-90.69972', (-0.4, -90.69972)),
	('.58306/-90.73306', (0.58306, -90.73306)),
	('0/-90.5', (0.0, -90.5)),
	('-0.750714/-90.306177', (-0.750714, -90.306177)),
	('-0.7594900000, -90.2786100000', (-0.75949, -90.27861)),
	('012700s;0894000w', (-1.45, -89.66666666666667)),
	('090230s;0910600w', (-9.041666666666666, -91.1)),
	('0 11.83s 91 47.33w', (-0.19716666666666666, -91.78883333333333)),
	('0 13s 91 45w', (-0.21666666666666667, -91.75)),
	('0 13.25s 91 44.50w', (-0.22083333333333333, -91.74166666666666)),
	('0,5°s 91°w', (-0.5, -91.0)),
	('9\' s, 91° 45\' 30" w', (-0.15, -91.75833333333334)),
	('14\' s, 91° 49\' 30" w', (-0.23333333333333334, -91.825)),
	('13\' 30" s, 91° 48\' 15" w', (-0.225, -91.80416666666666)),
	("01° 21.5' s 89° 38.7' w", (-1.3583333333333334, -89.645)),
	('0,6451°s 90,3454°w', (-0.6451, -90.3454)),
	('0,6437°s 90,3244°w', (-0.6437, -90.3244)),
	("00° 37' 05''  s  90° 24' 19''  w", (-0.6180555555555556, -90.40527777777778)),
	('(1° 30\' 29.88" n, 89° 30\' e)', (1.5083, 89.5)),
	('0 13s 91 47.50w', (-0.21666666666666667, -91.79166666666667)),
	('0° 29\' 20" s 90° 17\' 40" w', (-0.4888888888888889, -90.29444444444444)),
	('0° 45\' 06" s 90° 15\' 38" w', (-0.7516666666666667, -90.26055555555556)),
	("0° 25' s 90° 42' w", (-0.4166666666666667, -90.7)),
	("0 13' s., 90 42' w.", (-0.21666666666666667, -90.7)),
	('0 23\' 30" s., 90 17\' 40" w.', (-0.3916666666666667, -90.29444444444444)),
	('0 29\' 20" s., 90 17\' 40" w.', (-0.4888888888888889, -90.29444444444444)),
	('003300n;0904500w', (0.55, -90.75)),
	('002000n;0903000w', (0.3333333333333333, -90.5)),
	("1 40' s,  91 20' w", (-1.6666666666666665, -91.33333333333333)),
	('0° 35\' 50" s 90° 39\' 15" w', (-0.5972222222222222, -90.65416666666667)),
	('0° 35\' 50" s, 90° 39\' 15" w', (-0.5972222222222222, -90.65416666666667)),
	('0d 0m 0s s/90d 30m 0s w', (-0.0, -90.5)),
	('0d 30m 0s s/90d 30m 0s w', (-0.5, -90.5)),
	('91° 47\' 30"w, 0° 13\' 0"s', (-0.21666666666666667, -91.79166666666667)),
	('0° 12\' 35" s 91° 47\' 5" w', (-0.20972222222222223, -91.78472222222221)),
	("02 deg 46'n, 91 deg 46'w", (2.7666666666666666, -91.76666666666667)),
	('0.6667° s,  90.25° w', (-0.6667, -90.25)),
	("11' s,  90° 31' w", (-0.18333333333333332, -90.51666666666667)),
	("42' s,  90° 15' w", (-0.7, -90.25)),
	('.614162/-90.670756', (0.614162, -90.670756)),
	("1°17'51''s 90°26'3''w", (-1.2974999999999999, -90.43416666666667)),
	("00°30's 91°04'w", (-0.5, -91.06666666666666)),
	("90° 24' 19'  w 00° 37' 05'  s", (-0.6180555555555556, -90.40527777777778)),
	('0.74°s, 90.31°w', (-0.74, -90.31)),
]

lon_tests = [
	('90 13 18 w', -90.22166666666666),
	('09023 w', -90.38333333333334),
	('91 26\'50"w', -91.44722222222222),
	('08757 w', -87.95),
	('90 34.9700 w', -90.58283333333333),
	('0913848w', -91.64666666666668),
	('089 42 w', -89.7),
	('90 20 17.5 w', -90.33819444444444),
	('0912255w', -91.38194444444444),
	('0894530w', -89.75833333333334),
	('09158--w', -91.96666666666667),
	('092 w', -92.0),
	('89 43.5 w', -89.725),
	("89 38.7'w", -89.645),
	('-89.5', -89.5),
	('089 57 -- w', -89.95),
	('-90.26667', -90.26667),
	('090 26 18.00 w', -90.43833333333333),
	("89°30'e", 89.5),
	("88° 38' 36'' w", -88.64333333333335),
	("90° 17' w", -90.28333333333333),
	("90° 19' 0 w", -90.31666666666666),
	("91°1'w", -91.01666666666667),
	('091   w', -91.0),
	("91°0'w", -91.0),
	('-91.992074°', -91.992074),
	('091 24 -- w', -91.4),
	('090 16 15.60 w', -90.271),
	('90°29`w', -90.48333333333333),
	('89°57’13”w', -89.95361111111112),
	('w89°20′', -89.33333333333333),
	('90:02:13 w', -90.03694444444444),
	('ca. 90 18 58 w', -90.31611111111111),
]

def test():
	resolver = LatLonResolver()
	ok = True
	for (test, expected) in latlon_tests:
		result = resolver.parse_human_latlon(test)
		if result != expected:
			print(f"Test failure: parsing {test!r} yielded {result!r}; expected {expected!r}")
			ok = False
	for (test, expected) in lon_tests:
		result = resolver.parse_human_lon(test)
		if result != expected:
			print(f"Test failure: parsing {test!r} yielded {result!r}; expected {expected!r}")
			ok = False
	return ok
