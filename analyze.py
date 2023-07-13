# GBIF Darwin Core (DwC) Export
# Each line represents a column header in the dataset, followed by an educated guess about the data type

# 0: gbifID - (integer) Unique identifier assigned by GBIF
# 1: abstract - (string) A summary of the content of the resource
# 2: accessRights - (string) Information about who can access the resource and associated restrictions
# 3: accrualMethod - (string) The method by which items are added to a collection
# 4: accrualPeriodicity - (string) The frequency with which items are added to a collection
# 5: accrualPolicy - (string) The policy governing the addition of items to a collection
# 6: alternative - (string) An alternative name for the resource
# 7: audience - (string) A target group associated with the resource
# 8: available - (date) Date (often a range) of formal issuance (e.g., publication) of the resource
# 9: bibliographicCitation - (string) A bibliographic reference for the resource as a statement indicating how it should be cited (attributed) when used
# 10: conformsTo - (string) An established standard to which the described resource conforms
# 11: contributor - (string) An entity responsible for making contributions to the resource
# 12: coverage - (string) The spatial or temporal topic of the resource
# 13: created - (date) Date of creation of the resource
# 14: creator - (string) An entity primarily responsible for making the resource
# 15: date - (date) A point or period of time associated with an event in the lifecycle of the resource
# 16: dateAccepted - (date) Date on which the resource was accepted for publication
# 17: dateCopyrighted - (date) Date on which the resource was copyrighted
# 18: dateSubmitted - (date) Date on which the resource was submitted
# 19: description - (string) An account of the resource
# 20: educationLevel - (string) A class of entity for whom the resource is intended or useful
# 21: extent - (string) The size or duration of the resource
# 22: format - (string) The file format, physical medium, or dimensions of the resource
# 23: hasFormat - (string) A related resource that is substantially the same as the pre-existing described resource, but in another format
# 24: hasPart - (string) A related resource that is included either physically or logically in the described resource
# 25: hasVersion - (string) A related resource that is a version, edition, or adaptation of the described resource
# 26: identifier - (string) An unambiguous reference to the resource within a given context
# 27: instructionalMethod - (string) A process, used to engender knowledge, attitudes and skills, that the described resource is designed to support
# 28: isFormatOf - (string) A related resource that is substantially the same as the described resource, but in a different format
# 29: isPartOf - (string) A related resource in which the described resource is physically or logically included
# 30: isReferencedBy - (string) A related resource that references, cites, or otherwise points to the described resource
# 31: isReplacedBy - (string) A related resource that supplants, displaces, or supersedes the described resource
# 32: isRequiredBy - (string) A related resource that requires the described resource to support its function, delivery, or coherence
# 33: isVersionOf - (string) A related resource of which the described resource is a version, edition, or adaptation
# 34: issued - (date) Date of formal issuance (e.g., publication) of the resource
# 35: language - (string) A language of the resource
# 36: license - (string) A legal document giving official permission to do something with the resource
# 37: mediator - (string) An entity that mediates access to the resource and for whom the resource is intended or useful
# 38: medium - (string) The material or physical carrier of the resource
# 39: modified - (date) Date on which the resource was changed
# 40: provenance - (string) A statement of any changes in ownership and custody of the resource since its creation that are significant for its authenticity, integrity, and interpretation
# 41: publisher - (string) An entity responsible for making the resource available
# 42: references - (string) A related resource that is referenced, cited, or otherwise pointed to by the described resource
# 43: relation - (string) A related resource
# 44: replaces - (string) A related resource that is supplanted, displaced, or superseded by the described resource
# 45: requires - (string) A related resource that is required by the described resource to support its function, delivery, or coherence
# 46: rights - (string) Information about rights held in and over the resource
# 47: rightsHolder - (string) A person or organization owning or managing rights over the resource
# 48: source - (string) A related resource from which the described resource is derived
# 49: spatial - (string) Spatial characteristics of the resource
# 50: subject - (string) The topic of the resource
# 51: tableOfContents - (string) A list of subunits of the resource
# 52: temporal - (string) Temporal characteristics of the resource
# 53: title - (string) A name given to the resource
# 54: type - (string) The nature or genre of the resource
# 55: valid - (date) Date (often a range) of validity of a resource
# 56: institutionID - (string) An identifier for the institution possessing the record
# 57: collectionID - (string) An identifier for the collection or dataset from which the record was derived
# 58: datasetID - (string) An identifier for the set of data
# 59: institutionCode - (string) The name (or acronym) in use by the institution having custody of the object(s) or information referred to in the record
# 60: collectionCode - (string) The name, acronym, coden, or symbol identifying the collection or data set from which the record was derived
# 61: datasetName - (string) The name identifying the data set from which the record was derived
# 62: ownerInstitutionCode - (string) The name (or acronym) in use by the institution having ownership of the object(s) or information referred to in the record
# 63: basisOfRecord - (string) The specific nature of the data record
# 64: informationWithheld - (string) Information that has not been disclosed in a record for any of a number of reasons
# 65: dataGeneralizations - (string) Actions taken to make the shared data less specific or complete than in its original form
# 66: dynamicProperties - (string) A list (concatenated and separated) of additional measurements, facts, characteristics, or assertions about the record
# 67: occurrenceID - (string) An identifier for the Occurrence (as opposed to a particular digital record of the occurrence)
# 68: catalogNumber - (string) An identifier (preferably unique) for the record within the data set or collection
# 69: recordNumber - (string) An identifier given to the Occurrence at the time it was recorded
# 70: recordedBy - (string) A list (concatenated and separated) of names of people, groups, or organizations responsible for recording the original Occurrence
# 71: recordedByID - (string) A list (concatenated and separated) of identifiers (publication, global unique identifier, URI) of people, groups, or organizations responsible for recording the original Occurrence
# 72: individualCount - (integer) The number of individuals represented present at the time of the Occurrence
# 73: organismQuantity - (string) A number or enumeration value for the quantity of organisms
# 74: organismQuantityType - (string) The type, nature, or use of the measurement of the quantity of organisms
# 75: sex - (string) The sex of the biological individual(s) represented in the Occurrence
# 76: lifeStage - (string) The age class or life stage of the biological individual(s) at the time the Occurrence was recorded
# 77: reproductiveCondition - (string) The reproductive condition of the biological individual(s) represented in the Occurrence
# 78: behavior - (string) A description of the behavior shown by the subject at the time the Occurrence was recorded
# 79: establishmentMeans - (string) The process by which the biological individual(s) became established at the location
# 80: degreeOfEstablishment - (string) The degree to which the biological individual(s) have become established at the location
# 81: pathway - (string) The means of moving or transporting the resource into its new distribution
# 82: georeferenceVerificationStatus - (string) The verification status applied to the georeference
# 83: occurrenceStatus - (string) A statement about the presence or absence of a Taxon at a Location
# 84: preparations - (string) A list (concatenated and separated) of preparations and preservation methods for a specimen
# 85: disposition - (string) The current state of a specimen with respect to the collection identified in collectionCode or collectionID
# 86: associatedMedia - (string) A list (concatenated and separated) of identifiers (publication, global unique identifier, URI) for media associated with the Occurrence
# 87: associatedOccurrences - (string) A list (concatenated and separated) of identifiers or references to other Occurrences
# 88: associatedReferences - (string) A list (concatenated and separated) of identifiers (publication, global unique identifier, URI) for literature associated with the Occurrence
# 89: associatedSequences - (string) A list (concatenated and separated) of identifiers (publication, global unique identifier, URI) for genetic sequence information associated with the Occurrence
# 90: associatedTaxa - (string) A list (concatenated and separated) of identifiers or names of taxa and their associations with the Occurrence
# 91: otherCatalogNumbers - (string) A list (concatenated and separated) of previous or alternate fully qualified catalog numbers and other identifiers of the same Occurrence, whether in the current or any other data set or collection
# 92: occurrenceRemarks - (string) Comments or notes about the Occurrence
# 93: organismID - (string) An identifier for the set of organism information (data associated with the Organism class)
# 94: organismName - (string) A textual name for the organism
# 95: organismScope - (string) An indicator of the kind of Organism instance
# 96: associatedOrganisms - (string) A list (concatenated and separated) of identifiers or names of organisms and their associations with the Organism
# 97: previousIdentifications - (string) A list (concatenated and separated) of previous assignments of names to the Organism
# 98: organismRemarks - (string) Notes or comments about the Organism
# 99: materialSampleID - (string) An identifier for the MaterialSample (as opposed to a particular digital record of the MaterialSample)
# 100: eventID - (string) An identifier for the set of information associated with an Event (something that occurs at a place and time)
# 101: parentEventID - (string) An identifier for the broader Event that the Event identified by the record is a part of
# 102: fieldNumber - (string) An identifier given to the event in the field
# 103: eventDate - (date) The date-time or interval during which an Event occurred
# 104: eventTime - (string) The time or interval during which an Event occurred
# 105: startDayOfYear - (integer) The earliest ordinal day of the year on which the Event occurred
# 106: endDayOfYear - (integer) The latest ordinal day of the year on which the Event occurred
# 107: year - (integer) The four-digit year in which the Event occurred
# 108: month - (integer) The ordinal month in which the Event occurred
# 109: day - (integer) The integer day of the month on which the Event occurred
# 110: verbatimEventDate - (string) The verbatim original representation of the date and time information for an Event
# 111: habitat - (string) A category or description of the habitat in which the Event occurred
# 112: samplingProtocol - (string) The name of, reference to, or description of the method or protocol used during an Event
# 113: sampleSizeValue - (decimal) A numeric value for a measurement of the size (time duration, length, area, volume, weight, or number of items) of a sample in a sampling event
# 114: sampleSizeUnit - (string) The unit of measurement of the size (time duration, length, area, volume, weight, or number of items) of a sample in a sampling event
# 115: samplingEffort - (string) The amount of effort expended during an Event
# 116: fieldNotes - (string) Any information recorded in field notes
# 117: eventRemarks - (string) Comments or notes about the Event
# 118: locationID - (string) An identifier for the set of location information (data associated with dcterms:Location)
# 119: higherGeographyID - (string) An identifier for the geographic region within which the Location occurred
# 120: higherGeography - (string) A list (concatenated and separated) of geographic names less specific than the information captured in the locality term
# 121: continent - (string) The name of the continent in which the Location occurs
# 122: waterBody - (string) The name of the water body in which the Location occurs
# 123: islandGroup - (string) The name of the island group in which the Location occurs
# 124: island - (string) The name of the island on or near which the Location occurs
# 125: country - (string) The name of the country in which the Location occurs
# 126: countryCode - (string) The standard code for the country in which the Location occurs
# 127: stateProvince - (string) The name of the next smaller administrative region than country (state, province, etc.) in which the Location occurs
# 128: county - (string) The full, unabbreviated name of the next smaller administrative region than stateProvince (county, shire, department, etc.) in which the Location occurs
# 129: municipality - (string) The full, unabbreviated name of the next smaller administrative region than county (city, municipality, etc.) in which the Location occurs
# 130: locality - (string) The specific description of the place. Less specific geographic information can be provided in other geographic terms (higherGeography, continent, country, stateProvince, county, municipality, waterBody, island, islandGroup). This term may contain information modified from the original to obscure the precise location of vulnerable species
# 131: verbatimLocality - (string) The original textual description of the place
# 132: minimumElevationInMeters - (decimal) The lower limit of the range of elevation (altitude, usually above sea level), in meters
# 133: maximumElevationInMeters - (decimal) The upper limit of the range of elevation (altitude, usually above sea level), in meters
# 134: verbatimElevation - (string) The original description of the elevation (altitude, usually above sea level) as it appears on the specimen label or in the field notebook
# 135: depth - (decimal) The depth below the local surface, in meters
# 136: verbatimDepth - (string) The original description of the depth below the local surface as it appears on the specimen label or in the field notebook
# 137: minimumDistanceAboveSurfaceInMeters - (decimal) The lesser distance in a range of distance from a reference surface in the vertical direction, in meters
# 138: maximumDistanceAboveSurfaceInMeters - (decimal) The greater distance in a range of distance from a reference surface in the vertical direction, in meters
# 139: locationAccordingTo - (string) Information about the source of this Location information. Could be a publication (gazetteer), institution, or team of individuals.
# 140: locationRemarks - (string) Comments or notes about the Location
# 141: verbatimCoordinates - (string) The verbatim original spatial coordinates of the Location
# 142: verbatimLatitude - (string) The verbatim original latitude of the Location
# 143: verbatimLongitude - (string) The verbatim original longitude of the Location
# 144: verbatimCoordinateSystem - (string) The spatial coordinate system for the verbatimLatitude and verbatimLongitude or the verbatimCoordinates of the Location
# 145: verbatimSRS - (string) The ellipsoid, geodetic datum, or spatial reference system (SRS) upon which coordinates given in verbatimLatitude and verbatimLongitude, or verbatimCoordinates are based
# 146: decimalLatitude - (decimal) The geographic latitude (in decimal degrees, using the spatial reference system given in geodeticDatum or srsName) of the geographic center of a Location. Positive values are north of the Equator, negative values are south of it. Legal values lie between -90 and 90, inclusive.
# 147: decimalLongitude - (decimal) The geographic longitude (in decimal degrees, using the spatial reference system given in geodeticDatum or srsName) of the geographic center of a Location. Positive values are east of the Greenwich Meridian, negative values are west of it. Legal values lie between -180 and 180, inclusive.
# 148: geodeticDatum - (string) The ellipsoid, geodetic datum, or spatial reference system (SRS) upon which the geographic coordinates given in decimalLatitude and decimalLongitude as based
# 149: coordinateUncertaintyInMeters - (decimal) The horizontal distance (in meters) from the given decimalLatitude and decimalLongitude describing the smallest circle containing the whole of the Location. Note that coordinateUncertaintyInMeters is the radius, not the diameter of this circle.
# 150: coordinatePrecision - (decimal) A decimal representation of the precision of the coordinates given in the decimalLatitude and decimalLongitude
# 151: pointRadiusSpatialFit - (decimal) The ratio of the area of the point-radius (decimalLatitude, decimalLongitude, coordinateUncertaintyInMeters) to the area of the true (original, or most specific) spatial representation of the Location. Legal values lie between 0 (perfect circle of uncertainty touches the edge of the polygon) and 1 (the circle of uncertainty contains the entire polygon), inclusive.
# 152: footprintWKT - (string) A Well-Known Text (WKT) representation of the shape (footprint, geometry) that defines the Location. A Location may have both a point-radius representation (see decimalLatitude, decimalLongitude, coordinateUncertaintyInMeters) and a footprint representation, and they may differ from each other.
# 153: footprintSRS - (string) A Well-Known Text (WKT) representation of the Spatial Reference System (SRS) for the footprintWKT of the Location
# 154: footprintSpatialFit - (decimal) The ratio of the spatial footprint (footprintWKT) to the true (original, or most specific) spatial representation of the Location. Legal values lie between 0 (footprint represents an area completely outside of the original polygon) and 1 (footprint is identical to original polygon), inclusive.
# 155: georeferencedBy - (string) A list (concatenated and separated) of names of people, groups, or organizations who determined the georeference (spatial representation) for the Location
# 156: georeferencedDate - (date) The date on which the Location was georeferenced (i.e., the spatial representation was determined).
# 157: georeferenceProtocol - (string) A description or reference to the methods used to determine the spatial footprint, coordinates, and uncertainties
# 158: georeferenceSources - (string) A list (concatenated and separated) of maps, gazetteers, or other resources used to georeference the Location, described specifically enough to allow anyone in the future to use the same resources
# 159: georeferenceVerificationStatus - (string) The currentness of the georeference information
# 160: georeferenceRemarks - (string) Notes or comments about the georeferencing

### Geological Context (10 terms)
# 161: earliestEonOrLowestEonothem - (string) The full name of the earliest possible geochronologic eon or lowest chrono-stratigraphic eonothem or the informal name ("Precambrian") attributable based on the established stratigraphy.
# 162: latestEonOrHighestEonothem - (string) The full name of the latest possible geochronologic eon or highest chronostratigraphic eonothem or the informal name ("Precambrian") attributable based on the established stratigraphy.
# 163: earliestEraOrLowestErathem - (string) The full name of the earliest possible geochronologic era or lowest chronostratigraphic erathem attributable based on the established stratigraphy.
# 164: latestEraOrHighestErathem - (string) The full name of the latest possible geochronologic era or highest chronostratigraphic erathem attributable based on the established stratigraphy.
# 165: earliestPeriodOrLowestSystem - (string) The full name of the earliest possible geochronologic period or lowest chronostratigraphic system attributable based on the established stratigraphy.
# 166: latestPeriodOrHighestSystem - (string) The full name of the latest possible geochronologic period or highest chronostratigraphic system attributable based on the established stratigraphy.
# 167: earliestEpochOrLowestSeries - (string) The full name of the earliest possible geochronologic epoch or lowest chronostratigraphic series attributable based on the established stratigraphy.
# 168: latestEpochOrHighestSeries - (string) The full name of the latest possible geochronologic epoch or highest chronostratigraphic series attributable based on the established stratigraphy.
# 169: earliestAgeOrLowestStage - (string) The full name of the earliest possible geochronologic age or lowest chronostratigraphic stage attributable based on the established stratigraphy.
# 170: latestAgeOrHighestStage - (string) The full name of the latest possible geochronologic age or highest chronostratigraphic stage attributable based on the established stratigraphy.
# 171: lowestBiostratigraphicZone - (string) The full name of the lowest possible geological biostratigraphic zone of the stratigraphic horizon from which the cataloged item was collected.
# 172: highestBiostratigraphicZone - (string) The full name of the highest possible geological biostratigraphic zone of the stratigraphic horizon from which the cataloged item was collected.
# 173: lithostratigraphicTerms - (string) The combination (concatenated and separated) of all litho-stratigraphic names for the rock from which the cataloged item was collected.
# 174: group - (string) The full name of the lithostratigraphic group from which the cataloged item was collected.
# 175: formation - (string) The full name of the lithostratigraphic formation from which the cataloged item was collected.
# 176: member - (string) The full name of the lithostratigraphic member from which the cataloged item was collected.
# 177: bed - (string) The full name of the lithostratigraphic bed from which the cataloged item was collected.
# 178: geologicalContextID - (string) An identifier for the set of information associated with a GeologicalContext (the location within a geological context, such as stratigraphy). May be a global unique identifier or an identifier specific to the data set.
# 179: geologicalContextRemarks - (string) Comments or notes about the GeologicalContext

### Identification (5 terms)
# 180: identificationID - (string) An identifier for the Identification (the association of a Taxon to the Occurrence). May be a global unique identifier or an identifier specific to the data set.
# 181: identificationQualifier - (string) A brief phrase or a standard term ("cf.", "aff.") to express the determiner's doubts about the Identification.
# 182: typeStatus - (string) A statement about the original determination of the type.
# 183: identifiedBy - (string) A list (concatenated and separated) of names of people, groups, or organizations who assigned the Taxon to the subject.
# 184: dateIdentified - (date) The date on which the subject was identified as representing the Taxon. Recommended best practice is to use an encoding scheme, such as ISO 8601:2004(E).
# 185: identificationReferences - (string) A list (concatenated and separated) of references (publication, global unique identifier, URI) used in the Identification.
# 186: identificationVerificationStatus - (string) A categorical indicator of the extent to which the taxonomic identification has been verified to be correct.
# 187: identificationRemarks - (string) Comments or notes about the Identification.

### Taxon (13 terms)
# 188: taxonID - (string) An identifier for the Taxon assigned to the subject. May be a global unique identifier or an identifier specific to the data set.
# 189: scientificNameID - (string) An identifier for the nomenclatural (not taxonomic) details of a scientific name.
# 190: acceptedNameUsageID - (string) An identifier for the name that is currently in accepted use for the taxon.
# 191: parentNameUsageID - (string) An identifier for the parent taxonomic unit of the taxon in the taxonomic hierarchy.
# 192: originalNameUsageID - (string) An identifier for the original name usage (i.e., the name to which the protonym applies).
# 193: nameAccordingToID - (string) An identifier for the source in which the specific taxon concept circumscription is defined or implied - the source that defines the concept that the record represents.
# 194: namePublishedInID - (string) An identifier for the publication in which the scientificName was originally proposed.
# 195: taxonConceptID - (string) An identifier for the taxonomic concept to which the record refers - not the source of the taxon name.
# 196: scientificName - (string) The full scientific name, with authorship and date information if known. When forming part of an Identification, this should be the name in lowest level taxonomic rank that can be determined. This term should not contain identification qualifications, which should be supplied in the identificationQualifier term.
# 197: acceptedNameUsage - (string) The full name, with authorship and date information if known, of the currently valid (zoological) or accepted (botanical) taxon.
# 198: parentNameUsage - (string) The full name, with authorship and date information if known, of the direct, next higher taxonomic unit in the taxonomic hierarchy.
# 199: originalNameUsage - (string) The taxon name, with authorship and date information if known, as it originally appeared when first established, usually on the page of the original publication.
# 200: nameAccordingTo - (string) The reference to the source in which the specific taxon concept circumscription is defined or implied.
# 201: namePublishedIn - (string) A reference for the publication in which the scientificName was originally proposed.
# 202: namePublishedInYear - (int) The year in which the scientificName was published.
# 203: higherClassification - (string) A list (concatenated and separated) of taxa names terminating at the rank immediately superior to the taxon referenced in the taxon term.
# 204: kingdom - (string) The full scientific name of the kingdom in which the taxon is classified.
# 205: phylum - (string) The full scientific name of the phylum or division in which the taxon is classified.
# 206: class - (string) The full scientific name of the class in which the taxon is classified.
# 207: order - (string) The full scientific name of the order in which the taxon is classified.
# 208: family - (string) The full scientific name of the family in which the taxon is classified.
# 209: genus - (string) The full scientific name of the genus in which the taxon is classified.
# 210: subgenus - (string) The full scientific name of the subgenus in which the taxon is classified.
# 211: specificEpithet - (string) The name of the first or species epithet of the scientificName.
# 212: infraspecificEpithet - (string) The name of the lowest or terminal infraspecific epithet of the scientificName, excluding any rank designation.
# 213: taxonRank - (string) The taxonomic rank of the most specific name in the scientificName.
# 214: verbatimTaxonRank - (string) The taxonomic rank of the most specific name in the scientificName as it appears in the original record.
# 215: scientificNameAuthorship - (string) The authorship information for the scientificName formatted according to the conventions of the applicable nomenclaturalCode.
# 216: vernacularName - (string) A common or vernacular name.
# 217: nomenclaturalCode - (string) The nomenclatural code (or codes in the case of an ambiregnal name) under which the scientificName is constructed.
# 218: taxonomicStatus - (string) The status of the use of the scientificName as a label for a taxon. Requires taxonomic opinion to define the scope of a taxon. Rules of priority then are used to define the taxonomic status of the nomenclature contained in that concept.
# 219: nomenclaturalStatus - (string) The status related to the original publication of the name and its conformance to the relevant rules of nomenclature. It is based essentially on an algorithm according to the business rules of the code.
# 220: taxonRemarks - (string) Comments or notes about the taxon or name.

import os
import csv
import pandas as pd
import ratelim
from geopy.geocoders import Nominatim
from tabulate import tabulate
import unicodedata



class IslandFinder:
    def __init__(self, locality_name_file, island_name_file):

        self.abridged_filename = 'locality_only.tsv'

        with open(island_name_file, 'r') as file:
            self.island_names = [row for row in csv.reader(file)]

        self._load_data(locality_name_file, self.abridged_filename)

    def _load_data(self, original_file, abridged_file):
        if not os.path.exists(abridged_file):
            print(f"Creating abridged file with locality data only: {self.abridged_filename}")
            df = pd.read_csv(original_file, sep='\t', low_memory=False)
            self.df = df[[
                'gbifID',
                'locality',
                'verbatimLocality',
                'municipality',
                'locationRemarks',
                'verbatimCoordinates',
                'verbatimLongitude',
                'verbatimCoordinateSystem',
                'verbatimSRS',
                'decimalLatitude',
                'decimalLongitude',
                'geodeticDatum',
                'coordinateUncertaintyInMeters',
                'coordinatePrecision',
                'island',
                'islandGroup'
            ]]
            self.df = self.df.applymap(lambda x: sanitize(x) if isinstance(x, str) else x)
            self.df.insert(len(self.df.columns), 'interpolated_island', '')
            self.df.to_csv(self.abridged_filename, sep='\t', index=False)
        else:
            print(f"Loading previously created abdirged file: {self.abridged_filename}")

            self.df = pd.read_csv(self.abridged_filename, sep='\t')

    def search_locality_and_interpolate_island(self):
        for index, row in self.df.iterrows():
            for island in self.island_names:
                for synonym in island:
                    if isinstance(row['locality'], str) and sanitize(synonym) in sanitize(row['locality']) or isinstance(
                            row['verbatimLocality'], str) and sanitize(synonym) in sanitize(row['verbatimLocality']):
                        self.df.loc[index, 'interpolated_island'] = island[0]
                        break
                else:
                    continue
                break

    def save_data(self):
        not_missing_data = self.df[self.df['interpolated_island'].notnull()]
        not_missing_data.to_csv('found.tsv', sep='\t', index=False)

        missing_data = self.df[self.df['interpolated_island'].isnull()]
        missing_data.to_csv('not_found.tsv', sep='\t', index=False)

    def print_df(self, num_lines):
        headers = self.interpolated_df.columns.tolist()
        truncated_headers = [header[:100] + '...' if len(header) > 100 else header for header in headers]
        cols_to_drop = self.interpolated_df.columns[self.interpolated_df.head(num_lines).isna().all()].tolist()
        df_to_print = self.interpolated_df.drop(cols_to_drop, axis=1)
        df_to_print['gbifID'] = df_to_print['gbifID'].astype(int).astype(str)
        print(tabulate([truncated_headers], headers="keys", tablefmt="fancy_grid"))
        rows = df_to_print.head(num_lines).to_records(index=False)
        print(tabulate(rows, headers=truncated_headers, tablefmt="fancy_grid"))

    def normalize_interpolated(self):
        print("Normalizing the georeferenced strings")
        for index, row in self.df.iterrows():
            for island in self.island_names:
                for synonym in island:
                    if isinstance(row['interpolated_island'], str) and sanitize(synonym) in sanitize(row[
                        'interpolated_island']):
                        self.df.loc[index, 'interpolated_island'] = island[0]

    def geolocate_islands(self, geolocator, request_limit=10):
        requests_made = 0
        i = 0
        while i < len(self.df) and requests_made < request_limit:
            row = self.df.iloc[i]
            if pd.isnull(row['interpolated_island']):
                interpolated_island = get_location_by_coordinates(row['decimalLatitude'], row['decimalLongitude'],
                                                                  geolocator)
                self.df.at[i, 'interpolated_island'] = interpolated_island
                requests_made += 1
            i += 1
        self.normalize_interpolated()


@ratelim.greedy(1, 1)
def get_location_by_coordinates(decimal_latitude, decimal_longitude, geolocator):
    location = geolocator.reverse([decimal_latitude, decimal_longitude], exactly_one=True)
    address = location.raw['address']
    print(f"Got address {address} from {[decimal_latitude, decimal_longitude]}")
    return sanitize(address.get('county', ''))

def sanitize(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str.lower())
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode()

def main():
    geolocator = Nominatim(user_agent="geoapiExercises")

    finder = IslandFinder('./Galapagos_data/input/0080905-230530130749713/verbatim.txt', 'island_synonyms.csv')
    print("Checking locality strings for island matches...")

    finder.search_locality_and_interpolate_island()
    print("Geolocating [restricting to 10 localities for testing]...")

    finder.geolocate_islands(geolocator,10)
    print("Saving...")
    finder.save_data()


if __name__ == "__main__":
    main()
