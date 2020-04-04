import sys
import io
import os
import json
import argparse
import sqlite3
import pyproj
import concurrent.futures
import nutigeodb.osmimporter
import utils.tilemask as tilemask
from contextlib import closing

# Package URL template
DEFAULT_PACKAGE_URL_TEMPLATE = 'FULL_PACKAGE_URL/{version}/{id}.nutigeodb?appToken={{key}}'

# Default package version
DEFAULT_PACKAGE_VERSION = 1

# The number of parallel processes
MAX_WORKERS = 4 # Optimal in case of 32GB/48GB RAM

def importGeocodingDatabase(outputFileName, wofFileName, addressesFileName, highwaysFileName, buildingsFileName, dataDir, clipBounds, **kwargs):
  if os.path.exists(outputFileName):
    os.remove(outputFileName)

  with closing(sqlite3.connect('file:%s?mode=ro' % wofFileName, uri=True)) as wofDb:
    with closing(sqlite3.connect(outputFileName)) as db:
      db.isolation_level = None
      db.execute("PRAGMA locking_mode=EXCLUSIVE")
      db.execute("PRAGMA synchronous=OFF")
      db.execute("PRAGMA page_size=4096")
      db.execute("PRAGMA encoding='UTF-8'")

      importer = nutigeodb.osmimporter.OSMImporter(db, wofDb, addressesFileName, highwaysFileName, buildingsFileName, dataDir, clipBounds, **kwargs)
      importer.importPelias()
      importer.convertDatabase()

  with closing(sqlite3.connect(outputFileName)) as db:
    db.execute("VACUUM")
    db.execute("ANALYZE")

def processPackage(package, inputDir, wofFileName, dataDir, outputDir, **kwargs):
  outputFileName    = '%s/%s.nutigeodb'        % (outputDir, package['id'])
  addressesFileName = '%s/%s/addresses.txt.gz' % (inputDir,  package['id'])
  buildingsFileName = '%s/%s/buildings.txt.gz' % (inputDir,  package['id'])
  highwaysFileName  = '%s/%s/highways.txt.gz'  % (inputDir,  package['id'])
  if os.path.exists(outputFileName):
    if not os.path.exists(outputFileName + "-journal"):
      return outputFileName
    os.remove(outputFileName)
    os.remove(outputFileName + "-journal")

  print('Processing %s' % package['id'])
  try:
    epsg3857 = pyproj.Proj(init='epsg:3857')
    wgs84 = pyproj.Proj(init='epsg:4326')
    bounds = tilemask.tileMaskPolygon(package['tile_mask']).bounds
    clipPos0 = pyproj.transform(epsg3857, wgs84, bounds[0], bounds[1])
    clipPos1 = pyproj.transform(epsg3857, wgs84, bounds[2], bounds[3])
    clipBounds = (clipPos0[0], clipPos0[1], clipPos1[0], clipPos1[1])
    importGeocodingDatabase(outputFileName, wofFileName, addressesFileName, highwaysFileName, buildingsFileName, dataDir, clipBounds, **kwargs)
  except:
    if os.path.isfile(outputFileName):
      os.remove(outputFileName)
    raise
  return outputFileName

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='input', help='input directory (.tar.gz files)')
  parser.add_argument(dest='wof', help='name of Whosonfirst database file (.db)')
  parser.add_argument(dest='output', help='output directory for addresses')
  parser.add_argument('--packages', dest='packages', default=None, help='package filter (comma seperated)')
  parser.add_argument('--version', dest='version', default=DEFAULT_PACKAGE_VERSION, type=int, help='package version')
  parser.add_argument('--url_template', dest='url_template', default=DEFAULT_PACKAGE_URL_TEMPLATE, help='package URL template')
  parser.add_argument('--import-ids', dest='importIds', type=int, default=1, choices=[0, 1], help='import OSM ids')
  parser.add_argument('--import-postcodes', dest='importPostcodes', type=int, default=1, choices=[0, 1], help='import postcodes')
  parser.add_argument('--import-categories', dest='importCategories', type=int, default=1, choices=[0, 1], help='import POI categories')
  parser.add_argument('--import-wof', dest='importWOF', type=int, default=1, choices=[0, 1], help='import WhosOnFirst geometry')
  args = parser.parse_args()

  with io.open(args.template, 'rt', encoding='utf-8') as packagesFile:
    packagesTemplate = json.loads(packagesFile.read())
  packagesFilter = args.packages.split(',') if args.packages is not None else None
  packagesList = [package for package in packagesTemplate['packages'] if packagesFilter is None or package['id'] in packagesFilter]

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  os.makedirs(args.output, exist_ok=True)
  with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
    results = { package['id']: executor.submit(processPackage, package, args.input, args.wof, dataDir, args.output, importIds=args.importIds, importPostcodes=args.importPostcodes, importCategories=args.importCategories, importWOF=args.importWOF) for package in packagesList }

  outputFileNames = {}
  for packageId, result in results.items():
    try:
      outputFileNames[packageId] = result.result()
    except Exception as e:
      print('Package %s failed: %s' % (packageId, str(e)), file=sys.stderr)

  packagesList = []
  for package in packagesTemplate['packages']:
    if package['id'] in outputFileNames:
      outputFileName = outputFileNames[package['id']]
      statinfo = os.stat(outputFileName)
      package['version'] = args.version
      package['size'] = statinfo.st_size
      package['url'] = args.url_template.format(version=args.version, id=package['id'])
      packagesList.append(package)

  with io.open('%s/packages.json' % args.output, 'wt', encoding='utf-8') as packagesFile:
    packagesContainer = {
      'packages': packagesList,
      'metainfo': {}
    }
    packagesFile.write(json.dumps(packagesContainer))

if __name__ == "__main__":
  main()
