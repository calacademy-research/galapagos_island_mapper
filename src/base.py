import pandas

# Accuracy confidence
LOW = "low"
MODERATE = "moderate"
HIGH = "high"

class Resolution:
	def __init__(self, location, confidence, resolver):
		self.loc = location
		self.conf = confidence
		self.resolver = resolver
	def __repr__(self):
		return f"{self.loc!s} ({self.resolver}: {self.conf})"
	def fields(self):
		return {
			"island": self.loc,
			"confidence": self.conf,
			"resolver": self.resolver
		}
	def downgrade(self):
		if self.conf == MODERATE: self.conf = LOW
		if self.conf == HIGH: self.conf = MODERATE
	def upgrade(self):
		if self.conf == MODERATE: self.conf = HIGH
		if self.conf == LOW: self.conf = MODERATE

# Shortcut for "we don't know"
UNKNOWN = Resolution(None, LOW, None)

class Resolver:
	name = "base"
	def resolve(self, row):
		return [UNKNOWN]

class Table:
	def __init__(self, data, rows=None, columns=None, default=None):
		self.data = data
		if rows is None: self.rows = sorted(set(x[0] for x in data.keys()))
		else: self.rows = rows
		if columns is None: self.colums = sorted(set(x[1] for x in data.keys()))
		else: self.columns = columns
		self.default = default

	def get(self, row, col):
		if (row, col) in self.data: return self.data[(row, col)]
		return self.default

	def to_tsv(self, file):
		out = []
		for rowname in self.rows:
			row = [ self.get(rowname, colname) for colname in self.columns ]
			out.append([rowname] + row)
		pandas.DataFrame(out, columns=[[""] + self.columns]).to_csv(file, sep="\t", index=False)
