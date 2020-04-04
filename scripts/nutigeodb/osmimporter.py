import io
import re
import os
import os.path
import json
import gzip
import math
import pickle
import sqlite3
import nutigeodb.encodingstream as encodingstream
import nutigeodb.quadindex as quadindex
import nutigeodb.woflocator as woflocator
import nutigeodb.regexbuilder as regexbuilder
from nutigeodb.geomutils import *
from contextlib import closing
from tqdm import tqdm

# Misc parameters
SIMPLIFICATION_FACTOR = 1.0 / 256.0 # relative simplification factor
CLASS_TABLE = { 'country': 1, 'region': 2, 'county': 3, 'locality': 4, 'neighbourhood': 5, 'street': 6, 'postcode': 7, 'name': 8, 'housenumber': 9 }
RANK_SCALE = 32767
DEFAULT_EXTRA_POPULATION = 10
EXTRA_POPULATION_TABLE = { 'street': 100, 'neighbourhood': 1000, 'locality': 10000, 'county': 100000, 'region': 1000000, 'country': 10000000 }
MAX_GEOJSON_GEOMETRY_SIZE = 32 * 1024 * 1024

class OSMImporter(object):
  class Token(object):
    def __init__(self, dbid, name):
      self.dbid = dbid
      self.name = name
      self.typemask = 0
      self.count = 0
      self.idf = 0.0

  class Item(object):
    def __init__(self, type):
      self.type = type
      self.name = None
      self.extraNames = []
      self.geometry = None
      self.geomBoundsList = []
      self.properties = {}
      self.population = None
      self.dbids = {}

    @property
    def dbid(self):
      return self.dbids.get(self.type, None)

  class Entity(object):
    def __init__(self):
      self.housenumber = None
      self.geometry = None
      self.dbids = {}

  def __init__(self, db, wofDb, addressesFile, streetsFile=None, buildingsFile=None, dataDir='.', clipBounds=None, importIds=True, importPostcodes=True, importCategories=True, importWOF=True):
    self.db = db
    self.verbose = False
    self.addressesFile = addressesFile
    self.streetsFile = streetsFile
    self.buildingsFile = buildingsFile
    self.wofLocator = woflocator.WOFLocator(wofDb, [key for key, val in sorted(CLASS_TABLE.items(), key=lambda item: item[1])])
    self.dataDir = dataDir
    self.clipBounds = clipBounds
    self.importIds = importIds
    self.importPostcodes = importPostcodes
    self.importCategories = importCategories
    self.importWOF = importWOF
    self.nameCounter = 0
    self.categoryIds = {}
    self.streetsGeometry = {}
    self.buildingsGeometry = {}
    self.geomBounds = None
    self.tokenAbbrevs = {}
    self.toponyms = {}
    self.countryLangs = {}
    self.tokens = {}
    self.items = { 'country': {}, 'region': {}, 'county': {}, 'locality': {}, 'neighbourhood': {}, 'street': {}, 'postcode': {}, 'name': {} }
    self.itemId2Dbid = { 'country': {}, 'region': {}, 'county': {}, 'locality': {}, 'neighbourhood': {}, 'street': {}, 'postcode': {}, 'name': {} }
    self.dbid2ItemId = { 'country': {}, 'region': {}, 'county': {}, 'locality': {}, 'neighbourhood': {}, 'street': {}, 'postcode': {}, 'name': {} }
    with closing(io.open('%s/transliteration_table.json' % dataDir, 'rt', encoding='utf-8')) as f:
      self.transliterationTable = json.load(f)
    with closing(io.open('%s/iso3_to_iso2_langs.json' % dataDir, 'rt', encoding='utf-8')) as f:
      self.iso3ToISO2Langs = json.load(f)
    #self.housenumRegexBuilder = regexbuilder.RegexBuilder()

  def info(self, msg):
    if self.verbose:
      tqdm.write('Info: %s' % msg)

  def warning(self, msg):
    if self.verbose:
      tqdm.write('Warning: %s' % msg)

  def progress(self, iterable, **kwargs):
    if self.verbose:
      for item in tqdm(iterable, **kwargs):
        yield item
    else:
      for item in iterable:
        yield item

  def packCoordinates(self, coords):
    return pickle.dumps(coords)

  def unpackCoordinates(self, packedCoords):
    return pickle.loads(packedCoords)

  def normalizeName(self, name):
    for c in ('"', '%', '\\', '*', '(', ')', '[', ']', '{', '}', '=', ';', ',', '.', '!', '?', '|', '`', '~', '^', '_'):
      name = name.replace(c, ' ')
    return ' '.join([part for part in name.split() if part != ''])

  def tokenizeName(self, name):
    words = []
    for token in name.lower().split():
      word = ''
      for c in token:
        if c in self.transliterationTable:
          word += self.transliterationTable[c]
        else:
          word += c
      words.append(word)
    return words

  def normalizeHouseNumber(self, housenumber):
    return self.normalizeName(housenumber)

  def loadTokenAbbrevs(self, lang):
    fname = '%s/dictionaries/%s/street_types.txt' % (self.dataDir, lang)
    if not os.path.exists(fname):
      return {}
    tokenAbbrevs = {}
    with closing(io.open(fname, 'rt', encoding='utf-8')) as f:
      for line in f:
        names = [name.strip() for name in line.split('|')]
        for i in range(0, len(names)):
          for j in range(0, len(names)):
            if j != i:
              tokenAbbrevs[names[j]] = list(set(tokenAbbrevs.get(names[j], []) + [names[i]]))
    return tokenAbbrevs

  def loadToponyms(self, lang):
    fname = '%s/dictionaries/%s/toponyms.txt' % (self.dataDir, lang)
    if not os.path.exists(fname):
      return {}
    toponyms = {}
    with closing(io.open(fname, 'rt', encoding='utf-8')) as f:
      for line in f:
        names = line.split('|')
        for i in range(1, len(names)):
          name = names[0].lower()
          toponyms[name] = toponyms.get(name, []) + [names[i]]
    return toponyms

  def mapCountryToLanguage(self, isoCountry):
    if not self.countryLangs:
      fname = '%s/language/countries/country_language.tsv' % self.dataDir
      if not os.path.exists(fname):
        return None

      # Take only the main language currently (first one)
      with closing(io.open(fname, 'rt', encoding='utf-8')) as f:
        for line in f:
          elems = line.split()
          if len(elems) >= 2:
            country = elems[0].lower()
            if country not in self.countryLangs: 
              self.countryLangs[country] = elems[1].lower()
    return self.countryLangs.get(isoCountry.lower(), None) if isoCountry else None

  def calculateItemRank(self, item):
    rank = 1.0
    for parentType, parentDbid in item.dbids.items():
      if parentDbid is None:
        continue
      parentId = self.dbid2ItemId[parentType].get(parentDbid, None)
      if parentId in self.items[parentType]:
        population = (self.items[parentType][parentId].population or 0) + EXTRA_POPULATION_TABLE.get(parentType, DEFAULT_EXTRA_POPULATION)
        rank *= 1.0 - 1.0 / population
      else:
        self.warning('Item info missing when calculating rank')
    return rank

  def calculateEntityRank(self, entity):
    rank = 1.0
    for parentType, parentDbid in entity.dbids.items():
      if parentDbid is None:
        continue
      parentId = self.dbid2ItemId[parentType].get(parentDbid, None)
      if parentId in self.items[parentType]:
        population = (self.items[parentType][parentId].population or 0) + EXTRA_POPULATION_TABLE.get(parentType, DEFAULT_EXTRA_POPULATION)
        rank *= 1.0 - 1.0 / population
      else:
        self.warning('Entity info missing when calculating rank')
    return rank

  def createMetadataTables(self):
    self.db.execute("CREATE TABLE metadata (name TEXT NOT NULL, value TEXT NOT NULL, UNIQUE (name))")

  def createFieldTables(self):
    self.db.execute("CREATE TABLE names (id INTEGER NOT NULL, lang TEXT NULL, name TEXT NOT NULL, type INTEGER NOT NULL)")
    self.db.execute("CREATE TABLE nametokens (name_id INTEGER NOT NULL, token_id INTEGER NOT NULL)")

  def createFieldIndices(self):
    self.db.execute("CREATE INDEX names_id ON names (id)")
    self.db.execute("CREATE INDEX nametokens_token_id ON nametokens (token_id)")
    self.db.execute("CREATE INDEX nametokens_name_id ON nametokens (name_id)")

  def createTokenTables(self):
    self.db.execute("CREATE TABLE tokens (id INTEGER NOT NULL, token TEXT NOT NULL, idf REAL NOT NULL, typemask INTEGER NOT NULL)")

  def createTokenIndices(self):
    self.db.execute("CREATE INDEX tokens_id ON tokens (id)")
    self.db.execute("CREATE INDEX tokens_token ON tokens (token)")

  def createEntityTables(self):
    self.db.execute("CREATE TABLE entities (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, country_id INTEGER NULL, region_id INTEGER NULL, county_id INTEGER NULL, locality_id INTEGER NULL, neighbourhood_id INTEGER NULL, street_id INTEGER NULL, postcode_id INTEGER NULL, name_id INTEGER NULL, housenumbers TEXT NULL, features BLOB NOT NULL, quadindex INTEGER NOT NULL, rank REAL NOT NULL)")

  def createEntityIndices(self):
    self.db.execute("CREATE INDEX entities_localities ON entities (locality_id)")
    self.db.execute("CREATE INDEX entities_streets ON entities (street_id)")
    self.db.execute("CREATE INDEX entities_names ON entities (name_id)")
    self.db.execute("CREATE INDEX entities_quadindex ON entities (quadindex)")

  def createCategoryTables(self):
    self.db.execute("CREATE TABLE categories (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL)")
    self.db.execute("CREATE TABLE entitycategories (entity_id INTEGER NOT NULL, category_id INTEGER NOT NULL)")

  def createCategoryIndices(self):
    self.db.execute("CREATE INDEX entitycategories_entity_id ON entitycategories (entity_id)")

  def storeFieldNames(self):
    cursor = self.db.cursor()
    for type in CLASS_TABLE.keys():
      if type == 'housenumber':
        continue
      for item in self.items[type].values():
        if item.dbid is None:
          self.warning('Item %s missing id' % item.name)
          continue
        insertedValues = set()
        for lang, name in [(None, item.name)] + item.extraNames:
          cursor.execute("INSERT INTO names(id, lang, name, type) VALUES(?, ?, ?, ?)", (item.dbid, lang, name, CLASS_TABLE[type]))
          for tokenName in self.tokenizeName(name):
            tokenKey = (tokenName, lang)
            if tokenKey not in self.tokens:
              tokenKey = (tokenName, None)
            if tokenKey in self.tokens:
              values = (item.dbid, self.tokens[tokenKey].dbid)
              if values not in insertedValues:
                cursor.execute("INSERT INTO nametokens(name_id, token_id) VALUES(?, ?)", values)
                insertedValues.add(values)
            else:
              self.warning('Token %s missing' % tokenName)
    cursor.close()

  def importMetadata(self):
    cursor = self.db.cursor()
    version = 1
    cursor.execute("INSERT INTO metadata(name, value) VALUES(?, ?)", ('version', version))
    transTable = ",".join(['%s:%s' % (c, str(trans)) for c, trans in self.transliterationTable.items()])
    cursor.execute("INSERT INTO metadata(name, value) VALUES(?, ?)", ('translation_table', transTable))
    cursor.close()

  def importTokens(self, name, type, lang):
    if lang is not None and lang not in self.tokenAbbrevs:
      self.tokenAbbrevs[lang] = self.loadTokenAbbrevs(lang)
    for tokenName in self.tokenizeName(name):
      tokenKey = (tokenName, None)
      if lang is not None and tokenName in self.tokenAbbrevs[lang]:
        tokenKey = (tokenName, lang)
      if tokenKey not in self.tokens:
        self.tokens[tokenKey] = self.Token(len(self.tokens) + 1, tokenName)
      self.tokens[tokenKey].typemask = self.tokens[tokenKey].typemask | (1 << CLASS_TABLE[type])
      self.tokens[tokenKey].count += 1

  def storeTokens(self):
    totalCount = sum([token.count for token in self.tokens.values()])
    for token in self.tokens.values():
      token.idf = math.log(float(totalCount) / float(token.count))
    avgIDF = sum([token.idf / len(self.tokens) for token in self.tokens.values()])
    cursor = self.db.cursor()
    for tokenKey, token in self.tokens.items():
      tokenName, lang = tokenKey
      cursor.execute("INSERT INTO tokens(id, token, idf, typemask) VALUES(?, ?, ?, ?)", (token.dbid, tokenName, token.idf / avgIDF, token.typemask))
      for lang in self.tokenAbbrevs.keys():
        for tokenAbbrev in self.tokenAbbrevs[lang].get(tokenName, []):
          cursor.execute("INSERT INTO tokens(id, token, idf, typemask) VALUES(?, ?, ?, ?)", (token.dbid, tokenAbbrev, token.idf / avgIDF, token.typemask))
    cursor.close()

  def loadCategories(self, entityId):
    if not self.importCategories:
      return []
    cursor = self.db.cursor()
    cursor.execute("SELECT c.category FROM categories c, entitycategories ec WHERE c.id=ec.category_id and ec.entity_id=?", (entityId,))
    categories = [row[0] for row in cursor]
    cursor.close()
    return categories

  def storeCategories(self, entityId, categories):
    if not self.importCategories:
      return
    cursor = self.db.cursor()
    for category in categories:
      if category not in self.categoryIds:
        cursor.execute("INSERT INTO categories(category) VALUES(?)", (category,))
        self.categoryIds[category] = cursor.lastrowid
      cursor.execute("INSERT INTO entitycategories(entity_id, category_id) VALUES(?, ?)", (entityId, self.categoryIds[category]))
    cursor.close()

  def mapEntityParent(self, id, type):
    if id == -1:
      return None
    if id in self.itemId2Dbid[type]:
      return self.itemId2Dbid[type][id]

    self.nameCounter += 1
    dbid = self.nameCounter
    self.itemId2Dbid[type][id] = dbid
    self.dbid2ItemId[type][dbid] = id

    geojson = self.wofLocator.findGeoJSON(id)
    for n in range(10, 4, -1):
      if len(json.dumps(geojson['geometry'])) <= MAX_GEOJSON_GEOMETRY_SIZE:
        break
      geom = shapely.geometry.asShape(geojson['geometry'])
      geojson['geometry'] = shapely.geometry.mapping(geom.simplify(2.0**(-n), False))
    if geojson is None:
      self.warning('Failed to import WOF data for %s: %d' % (type, id))
      self.itemId2Dbid[type][id] = None
      return None
    properties = geojson.get('properties', {})
    geometry = geojson.get('geometry', None)
    if self.clipBounds is not None:
      if not testClipBounds(calculateGeometryBounds(geometry), self.clipBounds):
        self.warning('WOF geometry outside of clip bounds: %d' % id)
        self.itemId2Dbid[type][id] = None
        return None

    # Extract local language
    countryLang = self.mapCountryToLanguage(properties.get('wof:country', None))
    officialLangs = properties.get('wof:lang_x_official', [])
    if len(officialLangs) > 0:
      officialLang = self.iso3ToISO2Langs.get(officialLangs[0], None)
    else:
      officialLang = None

    # Extract and normalize name
    name = properties.get('wof:name', None)
    if name is None:
      self.warning('No WOF name for %s: %d' % (type, id))
      self.itemId2Dbid[type][id] = None
      return None
    if officialLang is not None:
      lang = properties.get('wof:lang_x_official')[0]
      names = properties.get('name:%s_x_preferred' % lang, [])
      if isinstance(names, list) and len(names) > 0:
        name = names[0]
    name = self.normalizeName(name)

    # Extract translations and alternative names
    translations = []
    for key, val in properties.items():
      match = re.match('name:(.*)_x_preferred', key)
      if match:
        lang = self.iso3ToISO2Langs.get(match.group(1), None)
        if lang and isinstance(val, list) and len(val) > 0:
          localName = self.normalizeName(val[0])
          if localName != name:
            translations.append((lang, localName))
    if countryLang not in self.toponyms:
      self.toponyms[countryLang] = self.loadToponyms(countryLang)
    toponyms = [(countryLang, toponym) for toponym in self.toponyms[countryLang].get(name.lower(), [])]

    # Create item record
    item = self.Item(type)
    item.name = name
    item.extraNames = translations + toponyms
    item.geometry = geometry
    item.population = properties.get('gn:population', None)
    item.dbids[type] = dbid

    # Try to merge with existing record, based on name
    for oldId, oldItem in self.items[type].items():
      if oldItem.name == item.name and oldItem.extraNames == item.extraNames:
        if not (item.population is None and oldItem.population is None):
          oldItem.population = (oldItem.population or 0) + (item.population or 0)
        if oldItem.geometry is not None and oldItem.geometry['type'].lower() == 'geometrycollection':
          geoms1 = oldItem.geometry['geometries']
        else:
          geoms1 = [oldItem.geometry] if oldItem.geometry is not None else []
        if item.geometry is not None and item.geometry['type'].lower() == 'geometrycollection':
          geoms2 = item.geometry['geometries']
        else:
          geoms2 = [item.geometry] if item.geometry is not None else []
        if len(geoms1 + geoms2) > 0:
          oldItem.geometry = { 'type': 'GeometryCollection', 'geometries': geoms1 + geoms2 }
        self.itemId2Dbid[type][id] = oldItem.dbid
        return oldItem.dbid

    # Extract full hierarchy of parents
    hierarchy = self.wofLocator.getHierarchy(id)
    if hierarchy:
      for parentField, parentId in hierarchy[0].items():
        parentType = parentField[:-3]
        item.dbids[parentType] = self.mapEntityParent(parentId, parentType)

    # Import tokens
    self.importTokens(name, type, officialLang or countryLang)
    for lang, extraName in item.extraNames:
      self.importTokens(extraName, type, lang)

    self.items[type][id] = item
    return dbid

  def mapEntityName(self, name, type, extraNames=[]):
    if name == '':
      return None
    name = self.normalizeName(name)
    extraNames = [(extraName[0], self.normalizeName(extraName[1])) for extraName in extraNames]
    for extraName in extraNames:
      self.importTokens(extraName[1], type, extraName[0])

    if extraNames and name in self.items[type]:
      for extraName in extraNames:
        if extraName not in self.items[type][name].extraNames:
          self.items[type][name].extraNames.append(extraName)

    if name in self.itemId2Dbid[type]:
      return self.itemId2Dbid[type][name]

    if name in self.items[type]:
      return self.items[type][name].dbid

    self.nameCounter += 1
    dbid = self.nameCounter
    self.itemId2Dbid[type][name] = dbid
    self.dbid2ItemId[type][dbid] = name

    # Create item
    item = self.Item(type)
    item.name = name
    item.extraNames = extraNames
    item.dbids[type] = dbid

    # Import tokens
    self.importTokens(name, type, None)

    self.items[type][name] = item
    return dbid

  def importStreetGeometry(self, osmData):
    type = osmData.get('type', None)
    if type != 'way':
      if type != 'node':
        self.warning('Ignored node type: %s' % type)
      return

    id = int(osmData['id'])
    nodes = osmData.get('nodes', [])
    coords = [(float(node['lon']), float(node['lat'])) for node in nodes]

    if len(coords) >= 2:
      self.streetsGeometry[id] = self.packCoordinates(coords)

  def importStreetGeometries(self):
    with closing(gzip.open(self.streetsFile, 'rb')) as f:
      lineCount = sum(1 for line in f)
    with closing(gzip.open(self.streetsFile, 'rb')) as f:
      for line in self.progress(f, total=lineCount):
        try:
          data = json.loads(line.decode('utf-8'))
        except:
          pass
        self.importStreetGeometry(data)

  def importBuildingGeometry(self, osmData):
    type = osmData.get('type', None)
    if type != 'way':
      if type != 'node':
        self.warning('Ignored node type: %s' % type)
      return

    id = int(osmData['id'])
    nodes = osmData.get('nodes', [])
    coords = [(float(node['lon']), float(node['lat'])) for node in nodes]

    if len(coords) >= 2:
      self.buildingsGeometry[id] = self.packCoordinates(coords)

  def importBuildingGeometries(self):
    with closing(gzip.open(self.buildingsFile, 'rb')) as f:
      lineCount = sum(1 for line in f)
    with closing(gzip.open(self.buildingsFile, 'rb')) as f:
      for line in self.progress(f, total=lineCount):
        try:
          data = json.loads(line.decode('utf-8'))
        except:
          pass
        self.importBuildingGeometry(data)

  def importWOFGeometries(self):
    cursor = self.db.cursor()
    itemCount = len(self.items)
    for type, items in self.progress(self.items.items(), total=itemCount):
      for id, item in items.items():
        if item.geometry is None:
          continue

        encodeStream = encodingstream.DeltaEncodingStream()
        encodeStream.encodeFeature({ 'id': id, 'geometry': item.geometry, 'properties': item.properties })

        nameId = 0
        if item.name in self.items['name']:
          nameId = self.items['name'][item.name].dbid
        cursor.execute('DELETE FROM entities WHERE country_id IS ? AND region_id IS ? AND county_id IS ? AND locality_id IS ? AND neighbourhood_id IS ? AND street_id IS ? AND (name_id IS NULL OR name_id=?)', (item.dbids.get('country', None), item.dbids.get('region', None), item.dbids.get('county', None), item.dbids.get('locality', None), item.dbids.get('neighbourhood', None), item.dbids.get('street', None), nameId))
        cursor.execute('INSERT INTO entities(country_id, region_id, county_id, locality_id, neighbourhood_id, street_id, postcode_id, housenumbers, name_id, features, quadindex, rank) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (item.dbids.get('country', None), item.dbids.get('region', None), item.dbids.get('county', None), item.dbids.get('locality', None), item.dbids.get('neighbourhood', None), item.dbids.get('street', None), None, None, None, sqlite3.Binary(encodeStream.getData()), 0, self.calculateItemRank(item)))
    cursor.close()

  def postProcessFeatures(self):
    # Calculate origin for geometry
    cursor1 = self.db.cursor()
    cursor1.execute('SELECT features FROM entities')
    geomOrigin = (0, 0)
    featureCounter = 0
    for row in cursor1:
      encodeStream = encodingstream.DeltaEncodingStream(row[0])
      while not encodeStream.eof():
        encodeStream.prevCoord = [0, 0]
        encodeStream.prevNumber = 0
        feature = encodeStream.decodeFeature()
        featureCounter += 1
        try:
          bounds = calculateGeometryBounds(feature['geometry'])
        except:
          continue
        origin = ((bounds[0] + bounds[2]) * 0.5, (bounds[1] + bounds[3]) * 0.5)
        geomOrigin = (geomOrigin[0] + (origin[0] - geomOrigin[0]) / featureCounter, geomOrigin[1] + (origin[1] - geomOrigin[1]) / featureCounter)

    # Create inverse mapping lists (dbid -> OSM id)
    itemOsmIds = {}
    for type, items in self.items.items():
      itemOsmIds[type] = {}
      for osmId, item in items.items():
        itemOsmIds[type][item.dbid] = osmId

    # Process geometries
    cursor1.execute('SELECT COUNT(*) FROM entities')
    rowCount = cursor1.fetchone()[0]
    cursor1.execute('SELECT id, features, housenumbers, country_id, region_id, county_id, locality_id, neighbourhood_id, street_id, postcode_id, housenumbers, name_id FROM entities ORDER BY id')
    cursor2 = self.db.cursor()
    for row in self.progress(cursor1, total=rowCount):
      entityId = row[0]

      # Read features and housenumbers into interleaved lists
      featureCollections = []
      housenumbers = []
      featureCounter = 0
      encodeStream = encodingstream.DeltaEncodingStream(row[1])
      while not encodeStream.eof():
        encodeStream.prevCoord = [0, 0]
        encodeStream.prevNumber = 0
        feature = encodeStream.decodeFeature()
        featureCounter += 1
        if not validateGeometry(feature['geometry']):
          self.warning('Geometry not valid: %d' % feature['id'])

        if row[2]:
          housenumber = self.normalizeHouseNumber(row[2].split('|')[featureCounter - 1])
          if housenumber not in housenumbers:
            housenumbers.append(housenumber)
            featureCollections.append([feature])
          else:
            featureCollections[housenumbers.index(housenumber)] += [feature]
        else:
          featureCollections.append([feature])

      # Add housenumbers to regex builder
      #if housenumbers:
      #  for housenumber in housenumbers:
      #    self.housenumRegexBuilder.add(housenumber)

      # Try to simplify and merge features
      for featureCollection in featureCollections:
        if not self.importIds:
          for i in range(0, len(featureCollection)):
            featureCollection[i]['id'] = 0

        i = 0
        while i < len(featureCollection):
          currentFeature = featureCollection[i]
          featureIndices = [i + j for j, feature in enumerate(featureCollection[i:]) if feature['id'] == currentFeature['id'] and feature['properties'] == currentFeature['properties']]
          geometry = mergeGeometries([featureCollection[j]['geometry'] for j in featureIndices]) if len(featureIndices) != 1 else featureCollection[i]['geometry']
          geometry = simplifyGeometry(geometry, SIMPLIFICATION_FACTOR) if not housenumbers else geometry
          featureCollection[i] = { 'id': currentFeature['id'], 'geometry': geometry, 'properties': currentFeature['properties'] }
          for j in reversed(featureIndices[1:]):
            featureCollection.pop(j)
          i += 1

      # Encode features
      encodeStream = encodingstream.DeltaEncodingStream(None, geomOrigin)
      for featureCollection in featureCollections:
        encodeStream.encodeFeatureCollection(featureCollection)

      # Calculate quadindex of all the geometries
      geometries = []
      for featureCollection in featureCollections:
        geometries += [feature['geometry'] for feature in featureCollection]
      try:
        bounds = calculateGeometryBounds({ 'type': 'GeometryCollection', 'geometries': geometries })
        quadIndex = quadindex.calculateGeometryQuadIndex(bounds)
      except:
        cursor2.execute("DELETE FROM entities WHERE id=?", (entityId,))
        self.warning('Removing entity %d due to illegal geometry' % entityId)
        continue

      for idx, field in [(idx, field) for idx, field in enumerate([description[0] for description in cursor1.description]) if field.endswith('_id')]:
        type = field[:-3]
        id = itemOsmIds.get(type, {}).get(row[idx], None)
        if id is not None:
          self.items[type][id].geomBoundsList = mergeBoundsLists(self.items[type][id].geomBoundsList, [bounds])

      # Update database
      self.geomBounds = mergeBounds(self.geomBounds, bounds)
      cursor2.execute('UPDATE entities SET features=?, housenumbers=?, quadindex=? WHERE id=?', (sqlite3.Binary(encodeStream.getData()), '|'.join(housenumbers) if housenumbers else None, quadIndex, entityId))

    if self.geomBounds is not None:
      cursor1.execute("INSERT INTO metadata(name, value) VALUES('bounds', '%.16g,%.16g,%.16g,%.16g')" % self.geomBounds)
    cursor1.execute("INSERT INTO metadata(name, value) VALUES('origin', '%.16g,%.16g')" % geomOrigin)
    cursor1.execute("INSERT INTO metadata(name, value) VALUES('encoding_precision', '%.16g')" % encodingstream.PRECISION)
    cursor1.execute("INSERT INTO metadata(name, value) VALUES('quadindex_level', '%d')" % quadindex.MAX_LEVEL)
    #cursor1.execute("INSERT INTO metadata(name, value) VALUES('housenumber_regex', '%s')" % self.housenumRegexBuilder.build())

    cursor2.close()
    cursor1.close()

  def importPeliasAddress(self, peliasData):
    if not 'data' in peliasData:
      return
    data = peliasData['data']

    match = re.search('.*[:](\d+).*', peliasData['_id'])
    if not match:
      self.warning('Failed to get entity id')
      return
    id = int(match.group(1))

    # Find parent info from gazetter
    entity = self.Entity()
    if 'center_point' in data:
      hierarchy = self.wofLocator.findHierarchy((data['center_point']['lon'], data['center_point']['lat']))
      if hierarchy:
        for parentField, parentId in hierarchy[0].items():
          parentType = parentField[:-3]
          entity.dbids[parentType] = self.mapEntityParent(parentId, parentType)
      entity.geometry = { 'type': 'Point', 'coordinates': (data['center_point']['lon'], data['center_point']['lat']) }
    else:
      self.warning('No coordinates for entity: %d' % id)
      return

    # Check country
    if entity.dbids.get('country', None) is None:
      self.warning('No country for entity: %d' % id)
      return

    # Store address info
    if 'address_parts' in data:
      if data['address_parts'].get('street', None):
        entity.dbids['street'] = self.mapEntityName(data['address_parts']['street'], 'street')
      if data['address_parts'].get('number', None):
        if entity.dbids.get('street', None) is not None:
          self.importTokens(data['address_parts']['number'], 'housenumber', None)
          entity.housenumber = data['address_parts']['number']
        else:
          self.warning('Ignoring housenumber, as street info is missing: %d' % id)
      if data['address_parts'].get('zip', None):
        if entity.housenumber is not None and self.importPostcodes:
          entity.dbids['postcode'] = self.mapEntityName(data['address_parts']['zip'], 'postcode')

    # Extract optional name and geometry
    name = data.get('name', {}).get('default', '')
    if name.isnumeric():
      self.warning("Numeric name '%s' for entity: %d" % (name, id))
      return
    extraNames = []
    for key, val in data.get('name', {}).items():
      if key != 'default' and val and not val.isnumeric():
        extraNames.append((key, val))

    if entity.dbids.get('street', None) is not None:
      if entity.housenumber is not None and id in self.buildingsGeometry:
        entity.geometry = { 'type': 'Polygon', 'coordinates': [self.unpackCoordinates(self.buildingsGeometry[id])] }
      if name != '':
        streetNames = [data.get('address_parts', {}).get('street', '')]
        if entity.housenumber is not None:
          streetNames = ['%s %s' % (entity.housenumber, streetNames[0]), '%s %s' % (streetNames[0], entity.housenumber)]
        if name not in streetNames:
          entity.dbids['name'] = self.mapEntityName(name, 'name', extraNames)
    else:
      if entity.housenumber is None and id in self.streetsGeometry:
        entity.geometry = { 'type': 'LineString', 'coordinates': self.unpackCoordinates(self.streetsGeometry[id]) }
        entity.dbids['street'] = self.mapEntityName(name, 'street', extraNames)
      else:
        if name == '':
          self.warning('No name for entity: %d' % id)
          return
        entity.dbids['name'] = self.mapEntityName(name, 'name', extraNames)

    # Check entity validity
    if entity.geometry is None:
      self.warning('Failed to import geometry: %d' % id)
      return
    if self.clipBounds is not None:
      if not testClipBounds(calculateGeometryBounds(entity.geometry), self.clipBounds):
        self.warning('Geometry entity geometry outside of clip bounds: %d' % id)
        return

    # Try to merge data
    cursor = self.db.cursor()
    cursor.execute('SELECT id, features, housenumbers, postcode_id FROM entities WHERE country_id IS ? AND region_id IS ? AND county_id IS ? AND locality_id IS ? AND neighbourhood_id IS ? AND street_id IS ? AND name_id IS ? AND %s' % ('housenumbers IS NOT NULL' if entity.housenumber else 'housenumbers IS NULL'), (entity.dbids.get('country', None), entity.dbids.get('region', None), entity.dbids.get('county', None), entity.dbids.get('locality', None), entity.dbids.get('neighbourhood', None), entity.dbids.get('street', None), entity.dbids.get('name', None)))
    for row in cursor:
      categories = self.loadCategories(row[0])
      if set(categories) != set(data.get('category', [])):
        continue
      encodeStream = encodingstream.DeltaEncodingStream(row[1])
      encodeStream.encodeFeature({ 'id': id, 'geometry': entity.geometry, 'properties': {} })
      features = sqlite3.Binary(encodeStream.getData())
      housenumbers = row[2] + '|' + entity.housenumber.replace('|', ' ') if entity.housenumber else None
      cursor.execute('UPDATE entities SET features=?, housenumbers=?, postcode_id=? WHERE id=?', (features, housenumbers, row[3] or entity.dbids.get('postcode', None), row[0]))
      cursor.close()
      return

    # Merging not possible, store
    encodeStream = encodingstream.DeltaEncodingStream()
    encodeStream.encodeFeature({ 'id': id, 'geometry': entity.geometry, 'properties': {} })
    features = sqlite3.Binary(encodeStream.getData())
    housenumbers = entity.housenumber.replace('|', ' ') if entity.housenumber else None
    cursor.execute('INSERT INTO entities(country_id, region_id, county_id, locality_id, neighbourhood_id, street_id, postcode_id, name_id, housenumbers, features, quadindex, rank) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (entity.dbids.get('country', None), entity.dbids.get('region', None), entity.dbids.get('county', None), entity.dbids.get('locality', None), entity.dbids.get('neighbourhood', None), entity.dbids.get('street', None), entity.dbids.get('postcode', None), entity.dbids.get('name', None), housenumbers, features, 0, self.calculateEntityRank(entity)))
    self.storeCategories(cursor.lastrowid, data.get('category', []))
    cursor.close()

  def importPeliasAddresses(self):
    with closing(gzip.open(self.addressesFile, 'rb')) as f:
      lineCount = sum(1 for line in f)
    with closing(gzip.open(self.addressesFile, 'rb')) as f:
      for line in self.progress(f, total=lineCount):
        try:
          data = json.loads(line.decode('utf-8'))
        except:
          pass
        self.importPeliasAddress(data)

  def importPelias(self):
    self.db.execute("BEGIN")

    self.info('Creating tables')
    self.createMetadataTables()
    self.createTokenTables()
    self.createFieldTables()
    self.createEntityTables()
    self.createCategoryTables()

    self.importMetadata()

    self.db.execute("CREATE INDEX entities_all ON entities (country_id, region_id, county_id, locality_id, neighbourhood_id, street_id, postcode_id, name_id)")
    self.db.execute("CREATE INDEX entitycategories_entity_id ON entitycategories (entity_id)")

    if self.streetsFile is not None:
      self.info('Importing street geometry')
      self.importStreetGeometries()

    if self.buildingsFile is not None:
      self.info('Importing building geometry')
      self.importBuildingGeometries()

    self.info('Importing Pelias addresses')
    self.importPeliasAddresses()

    if self.importWOF:
      self.info('Importing WhosOnFirst geometry')
      self.importWOFGeometries()

    self.db.commit()

    self.db.execute("DROP INDEX entities_all")
    self.db.execute("DROP INDEX entitycategories_entity_id")

    self.db.execute("BEGIN")

    self.postProcessFeatures()

    self.info('Storing names and tokens')
    self.storeTokens()
    self.storeFieldNames()

    self.info('Creating indices')
    self.createTokenIndices()
    self.createFieldIndices()
    self.createEntityIndices()
    self.createCategoryIndices()

    self.db.commit()

  def convertDatabase(self):
    self.db.execute("BEGIN")

    cursor = self.db.cursor()
    cursor1 = self.db.cursor()
    cursor2 = self.db.cursor()

    # Set type
    cursor.execute("ALTER TABLE entities ADD type INTEGER NOT NULL DEFAULT 0")

    cursor1.execute("SELECT COUNT(*) FROM entities")
    rowCount = cursor1.fetchone()[0]
    cursor1.execute("SELECT rowid, housenumbers, country_id, region_id, county_id, locality_id, neighbourhood_id, street_id, NULL, name_id FROM entities")
    for row in self.progress(cursor1, total=rowCount):
      if row[-1]:
        type = CLASS_TABLE['name']
      elif row[1]:
        type = CLASS_TABLE['housenumber']
      elif row[-3]:
        type = CLASS_TABLE['street']
      elif row[-4]:
        type = CLASS_TABLE['neighbourhood']
      elif row[-5]:
        type = CLASS_TABLE['locality']
      elif row[-6]:
        type = CLASS_TABLE['county']
      elif row[-7]:
        type = CLASS_TABLE['region']
      elif row[-8]:
        type = CLASS_TABLE['country']
      cursor.execute("UPDATE entities SET type=? WHERE rowid=?", (type, row[0]))

    # Store name info in separate entitynames table
    cursor.execute("CREATE TABLE entitynames (entity_id INTEGER NOT NULL, name_id INTEGER NOT NULL)")
    for idx, field in enumerate(['country', 'region', 'county', 'locality', 'neighbourhood', 'street', 'postcode', 'name']):
      cursor.execute("INSERT INTO entitynames(entity_id, name_id) SELECT id, %s_id FROM entities WHERE %s_id IS NOT NULL" % (field, field))

    # Add entitycount field to names
    cursor.execute("CREATE INDEX entitynames_entity_name_id ON entitynames(entity_id, name_id)")
    cursor.execute("CREATE INDEX entitynames_name_id ON entitynames(name_id)")
    cursor.execute("ALTER TABLE names ADD entitycount INTEGER NOT NULL DEFAULT 0")
    cursor.execute("UPDATE names SET entitycount=(SELECT COUNT(*) FROM entitynames WHERE entitynames.name_id=names.id)")

    # Add namecount field to tokens
    cursor.execute("CREATE INDEX nametokens_token_name_id ON nametokens (token_id, name_id)")
    cursor.execute("ALTER TABLE tokens ADD namecount INTEGER NOT NULL DEFAULT 0")
    cursor.execute("UPDATE tokens SET namecount=(SELECT COUNT(*) FROM nametokens WHERE nametokens.token_id=tokens.id)")

    # Update housenumbers
    names = {}
    cursor1.execute("SELECT MAX(id) FROM names")
    nameId = cursor1.fetchone()[0] or 0
    cursor.execute("ALTER TABLE entities RENAME TO old_entities")
    cursor.execute("CREATE TABLE entities (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, type INTEGER NOT NULL, features BLOB NOT NULL, housenumbers BLOB NULL, quadindex INTEGER NOT NULL, rank INTEGER NOT NULL)")
    cursor.execute("INSERT INTO metadata(name, value) VALUES('rank_scale', ?)", (str(RANK_SCALE),))

    cursor1.execute("SELECT COUNT(*) FROM old_entities")
    rowCount = cursor1.fetchone()[0]
    cursor1.execute("SELECT id, type, quadindex, rank, housenumbers, features FROM old_entities")
    for row1 in self.progress(cursor1, total=rowCount):
      encodeStream = encodingstream.DeltaEncodingStream()
      if row1[4]:
        houseNums = row1[4].split("|")
        for houseNum in houseNums:
          if houseNum not in names:
            nameId += 1
            names[houseNum] = nameId
            cursor.execute("INSERT INTO names(id, lang, name, type) VALUES(?, ?, ?, ?)", (nameId, None, houseNum, 9))
            for token in self.tokenizeName(houseNum):
              cursor2.execute("SELECT id FROM tokens WHERE token=?", (token,))
              for row2 in cursor2:
                cursor.execute("INSERT INTO nametokens(name_id, token_id) VALUES(?, ?)", (nameId, row2[0]))
          encodeStream.encodeNumber(names[houseNum])
      cursor.execute("INSERT INTO entities(id, type, quadindex, rank, features, housenumbers) VALUES(?, ?, ?, ?, ?, ?)", (row1[0], row1[1], row1[2], int(row1[3] * RANK_SCALE), row1[5], sqlite3.Binary(encodeStream.getData()) if encodeStream.getData() else None))

    cursor.execute("DROP TABLE old_entities")

    # Add lang to nametokens
    cursor.execute("ALTER TABLE nametokens ADD lang TEXT NULL")
    cursor1.execute("SELECT id, name, lang FROM names WHERE id IN (SELECT id FROM names WHERE lang IS NOT NULL)")
    rows1 = cursor1.fetchall()
    cursor.execute("DELETE FROM nametokens WHERE name_id IN (SELECT id FROM names WHERE lang IS NOT NULL)")
    for row1 in rows1:
      for token in self.tokenizeName(row1[1]):
        cursor.execute("INSERT INTO nametokens(name_id, token_id, lang) SELECT ?, id, ? FROM tokens WHERE token=?", (row1[0], row1[2], token))

    # Indices
    cursor.execute("DROP INDEX nametokens_token_id")

    cursor.execute("CREATE INDEX entities_id ON entities(id)")
    cursor.execute("CREATE INDEX entities_type ON entities(type)")
    cursor.execute("CREATE INDEX entities_quadindex ON entities(quadindex)")

    # Done
    cursor2.close()
    cursor1.close()
    cursor.close()

    self.db.commit()
