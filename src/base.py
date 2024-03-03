
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
