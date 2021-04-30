# Import OSM addresses for geocoding from .osm.xml or .osm.pbf file.

import os
import io
import sys
import json
import gzip
import argparse
import tempfile
import sqlite3
import subprocess
import osmium
import shapely.geometry
import concurrent.futures
import utils.tilemask as tilemask
from contextlib import closing

class GeocodeExporter(osmium.SimpleHandler):
  def __init__(self, addressFile, buildingsFile, highwaysFile, categoryMap, tagList):
    osmium.SimpleHandler.__init__(self)
    self.addressFile = addressFile
    self.buildingsFile = buildingsFile
    self.highwaysFile = highwaysFile
    self.categoryMap = categoryMap
    self.addressMap = { 'addr:housenumber': 'number', 'addr:postcode': 'zip', 'addr:street': 'street' }
    self.tagList = tagList

  def _get_nodes(self, nodes):
    if len(nodes) == 0 or not all([node.location.valid() for node in nodes]):
      return None
    return [{ 'lat': node.location.lat, 'lon': node.location.lon } for node in nodes]

  def _valid_item(self, item):
    for tags in self.tagList:
      if all([tag in item.tags for tag in tags.split('+')]):
        return True
    return False

  def _get_categories(self, item):
    categories = set()
    for tag, values in self.categoryMap.items():
      if tag in item.tags:
        if item.tags[tag] in values:
          categories.update(values[item.tags[tag]])
    return None if len(categories) == 0 else list(categories)

  def _get_address(self, item):
    addr = {}
    for tag, key in self.addressMap.items():
      if tag in item.tags:
        addr[key] = item.tags[tag]
    return None if len(addr) == 0 else addr

  def node(self, n):
    if self._valid_item(n) and n.location.valid():
      item = { '_id': 'node:%d' % n.id, '_type': 'venue' }
      center = { 'lat': n.location.lat, 'lon': n.location.lon }
      data = { 'center_point': center }
      if 'name' in n.tags:
        data['name'] = { 'default': n.tags['name'] }
        for tag in n.tags:
          if tag.k.startswith('name:'):
            data['name'][tag.k[5:]] = tag.v 
      addr = self._get_address(n)
      if addr is not None:
        item['_type'] = 'address'
        data['address_parts'] = addr
      categories = self._get_categories(n)
      if categories is not None:
        data['category'] = categories
      item['data'] = data
      self.addressFile.write(json.dumps(item, indent=None) + "\n")

  def way(self, w):
    if self._valid_item(w) and len(w.nodes) > 0 and all([node.location.valid() for node in w.nodes]):
      item = { '_id': 'way:%d' % w.id, '_type': 'venue' }
      center = { 'lat': sum([node.location.lat for node in w.nodes]) / len(w.nodes), 'lon': sum([node.location.lon for node in w.nodes]) / len(w.nodes) }
      data = { 'center_point': center }
      if 'name' in w.tags:
        data['name'] = { 'default': w.tags['name'] }
        for tag in w.tags:
          if tag.k.startswith('name:'):
            data['name'][tag.k[5:]] = tag.v 
      addr = self._get_address(w)
      if addr is not None:
        item['_type'] = 'address'
        data['address_parts'] = addr
      categories = self._get_categories(w)
      if categories is not None:
        data['category'] = categories
      item['data'] = data
      self.addressFile.write(json.dumps(item, indent=None) + "\n")
    if 'building' in w.tags:
      nodes = self._get_nodes(w.nodes)
      if nodes is not None:
        item = { 'id': w.id, 'type': 'way', 'nodes': nodes }
        self.buildingsFile.write(json.dumps(item, indent=None) + "\n")
    elif 'highway' in w.tags:
      nodes = self._get_nodes(w.nodes)
      if nodes is not None:
        item = { 'id': w.id, 'type': 'way', 'nodes': nodes }
        self.highwaysFile.write(json.dumps(item, indent=None) + "\n")

def importPackage(package, inputDir, outputDir, categoryMap, tagList):
  inputFileName = '%s/%s.osm.pbf' % (inputDir, package['id'])
  addressesFileName = '%s/%s/addresses.txt.gz' % (outputDir, package['id'])
  buildingsFileName = '%s/%s/buildings.txt.gz' % (outputDir, package['id'])
  highwaysFileName  = '%s/%s/highways.txt.gz'  % (outputDir, package['id'])
  if os.path.exists(addressesFileName):
    return (addressesFileName, buildingsFileName, highwaysFileName)

  os.makedirs(os.path.dirname(addressesFileName), exist_ok=True)

  print('Importing data for %s' % package['id'])
  try:
    with closing(gzip.open(addressesFileName, mode='wt', encoding='utf-8')) as addressesFile:
      with closing(gzip.open(buildingsFileName, mode='wt', encoding='utf-8')) as buildingsFile:
        with closing(gzip.open(highwaysFileName, mode='wt', encoding='utf-8')) as highwaysFile:
          exporter = GeocodeExporter(addressesFile, buildingsFile, highwaysFile, categoryMap, tagList)
          exporter.apply_file(inputFileName, locations=True)
  except:
    for outputFileName in (addressesFileName, buildingsFileName, highwaysFileName):
      if os.path.isfile(outputFileName):
        os.remove(outputFileName)
    raise
  return (addressesFileName, buildingsFileName, highwaysFileName)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='input', help='input directory for package .osm.pbf files')
  parser.add_argument(dest='output', help='output directory for addresses')
  parser.add_argument('--packages', dest='packages', default=None, help='package filter (comma seperated)')
  args = parser.parse_args()

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  with io.open('%s/osm_category_map.json' % dataDir, 'rt', encoding='utf-8') as categoryFile:
    categoryMap = json.loads(categoryFile.read())
  with io.open('%s/osm_tag_list.json' % dataDir, 'rt', encoding='utf-8') as tagFile:
    tagList = json.loads(tagFile.read())

  with io.open(args.template, 'rt', encoding='utf-8') as packagesFile:
    packagesTemplate = json.loads(packagesFile.read())
  packagesFilter = args.packages.split(',') if args.packages is not None else None

  with concurrent.futures.ProcessPoolExecutor() as executor:
    results = { package['id']: executor.submit(importPackage, package, args.input, args.output, categoryMap, tagList) for package in packagesTemplate['packages'] if packagesFilter is None or package['id'] in packagesFilter }

  outputFileNames = {}
  for packageId, result in results.items():
    try:
      outputFileNames[packageId] = result.result()
    except Exception as e:
      print('Package %s failed: %s' % (packageId, str(e)), file=sys.stderr)
 
if __name__ == "__main__":
  main()
