
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
		return f"{self.loc!s} ({self.conf})"

# Shortcut for "we don't know"
UNKNOWN = Resolution(None, LOW, None)

class Resolver:
	name = "base"
	def resolve(self, _):
		return UNKNOWN
