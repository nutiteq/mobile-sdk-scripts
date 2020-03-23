import os
import io
import sys
import json
import csv
import gzip
import glob
import shutil
import pyproj
import shapely.geometry
import shapely.prepared
import argparse
import utils.tilemask as tilemask
import concurrent.futures
from contextlib import closing

def cleanupAddressFiles(outputDir, packageIds):
  for packageId in packageIds:
    outputPackageDir = '%s/%s' % (outputDir, packageId)
    if os.path.isdir(outputPackageDir):
      shutil.rmtree(outputPackageDir, ignore_errors=True)
    os.makedirs(outputPackageDir, exist_ok=True)

def processPOIAddressFile(inputFileName, outputDir, packageTileMaskPolygons, categoryIdFilter, qualityScoreThreshold, iso2ToISO3Langs):
  print('Processing %s' % inputFileName)
  wgs84 = pyproj.Proj(init='epsg:4326')
  epsg3857 = pyproj.Proj(init='epsg:3857')
  packageTileMaskPolygons = { packageId: shapely.prepared.prep(polygon) for packageId, polygon in packageTileMaskPolygons.items() }
  inputFileId = os.path.basename(inputFileName).split(".")[0]
  outputFiles = {}
  with closing(gzip.open(inputFileName, 'rt', encoding='utf-8')) as inputFile:
    reader = csv.DictReader(inputFile)
    for row in reader:
      id, categoryId, categoryName = row['id'], row['category_id'], row['category_name']
      nameOriginal = row['name_original']
      qualityRealityScore = float(row['quality_reality_score'])
      houseNum, addressOriginal = row.get('num_house', ''), row.get('address_original', '')
      nameMulti = json.loads(row.get('name_multi_json', {}) or row.get('name_multi', {}) or row.get('to_json', {}))

      if qualityRealityScore < qualityScoreThreshold:
        continue
      if any([categoryId.startswith(filterPrefix) for filterPrefix in categoryIdFilter]):
        continue
      categoryClass = '_'.join(part.strip() for part in categoryName.strip().lower().replace("'", "").replace('&', ' ').replace(',', ' ').replace('-', '_').replace('/', '_').split() if part.strip())
      geom = shapely.geometry.shape(json.loads(row['geojson']))

      itemId = 'node:%d' % (int(id[-12:], base=16) | (1 << 48))
      itemCentroid = { 'lon': geom.centroid.x, 'lat': geom.centroid.y }
      itemData = { 'name': { 'default': nameOriginal }, 'center_point': itemCentroid, 'source': 'here', 'layer': 'venue', 'source_id': id, 'category': [categoryClass] }
      itemType = 'venue'
      for lang, nameLang in nameMulti.items():
        if lang.lower() in iso2ToISO3Langs and nameLang != nameOriginal:
          itemData['name'][lang.lower()] = nameLang
      if addressOriginal and houseNum:
        itemType = 'address'
        itemData['address_parts'] = { 'number': houseNum, 'street': addressOriginal }
      item = { '_index': 'here', '_type': itemType, '_id': itemId, 'data': itemData }

      point = shapely.geometry.Point(*pyproj.transform(wgs84, epsg3857, geom.centroid.x, geom.centroid.y))
      for packageId, packagePolygon in packageTileMaskPolygons.items():
        if packagePolygon.contains(point):
          if packageId not in outputFiles:
            outputFileName = '%s/%s/venues_%s.txt.gz' % (outputDir, packageId, inputFileId)
            outputFiles[packageId] = gzip.open(outputFileName, 'wt', encoding='utf-8')
          outputFiles[packageId].write(json.dumps(item, indent=None))
          outputFiles[packageId].write("\n")
  for outputFile in outputFiles.values():
    outputFile.close()
  return list(outputFiles.keys())

def mergeAddressFiles(inputDir, outputDir, packageIds):
  print('Merging address files')
  for packageId in packageIds:
    outputFileName = '%s/%s/addresses.txt.gz' % (outputDir, packageId)
    with closing(gzip.open(outputFileName, 'wt', encoding='utf-8')) as outputFile:
      inputFileName = '%s/%s/addresses.txt.gz' % (inputDir, packageId)
      if os.path.isfile(inputFileName):
        with closing(gzip.open(inputFileName, 'rt', encoding='utf-8')) as inputFile:
          for row in inputFile:
            data = json.loads(row)
            if data['_type'] == 'venue' and data['_id'].startswith('node:'):
              continue
            outputFile.write(row)
      processedRowIds = set()
      inputFileNames = glob.glob("%s/%s/venues_*.txt.gz" % (outputDir, packageId))
      for inputFileName in inputFileNames:
        with closing(gzip.open(inputFileName, 'rt', encoding='utf-8')) as inputFile:
          for row in inputFile:
            rowId = json.loads(row)['_id']
            if rowId not in processedRowIds:
              processedRowIds.add(rowId)
              outputFile.write(row)
    for name in ['buildings.txt.gz', 'highways.txt.gz']:
      inputFileName = '%s/%s/%s' % (inputDir, packageId, name)
      if os.path.isfile(inputFileName):
        outputFileName = '%s/%s/%s' % (outputDir, packageId, name)
        try:
          os.symlink(inputFileName, outputFileName)
        except:
          shutil.copyfile(inputFileName, outputFileName)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='world', help='input directory for .txt.gz files')
  parser.add_argument(dest='poi', help='input directory for .csv.gz files')
  parser.add_argument(dest='output', help='output folder for addresses')
  parser.add_argument('--packages', dest='packages', default=None, help='package filter (comma seperated)')
  parser.add_argument('--filter', dest='filter', default=None, help='name for .txt file containing categories to filter out')
  parser.add_argument('--quality', dest='quality', type=float, default=0.0, help='quality threshold level')
  args = parser.parse_args()

  with io.open(args.template, 'rt', encoding='utf-8') as packagesFile:
    packagesTemplate = json.loads(packagesFile.read())
  packagesFilter = args.packages.split(',') if args.packages is not None else None
  packagesList = [package for package in packagesTemplate['packages'] if packagesFilter is None or package['id'] in packagesFilter]

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  with io.open('%s/iso3_to_iso2_langs.json' % dataDir, 'rt', encoding='utf-8') as f:
    iso2ToISO3Langs = { val: key for key, val in json.load(f).items() }
  if args.filter:
    with io.open(args.filter, mode='rt', encoding='utf-8') as f:
      categoryIdFilter = [row.strip() for row in f.readlines() if row.strip()]
  else:
    categoryIdFilter = []

  print('Initializing')
  os.makedirs(args.output, exist_ok=True)
  packageTileMaskPolygons = { package['id']: tilemask.tileMaskPolygon(package['tile_mask']) for package in packagesList }
  cleanupAddressFiles(args.output, packageTileMaskPolygons.keys())

  inputFileNames = glob.glob("%s/*.csv.gz" % args.poi)
  with concurrent.futures.ProcessPoolExecutor() as executor:
    results = { inputFileName: executor.submit(processPOIAddressFile, inputFileName, args.output, packageTileMaskPolygons, categoryIdFilter, args.quality, iso2ToISO3Langs) for inputFileName in inputFileNames }

  for inputFileName, result in results.items():
    try:
      outputFileNames = result.result()
    except Exception as e:
      print('Processing %s failed: %s' % (inputFileName, str(e)), file=sys.stderr)

  mergeAddressFiles(args.world, args.output, packageTileMaskPolygons.keys())

if __name__ == "__main__":
  main()
