# Script for building single large .mbtiles file containing all the POIs.

import os
import io
import sys
import json
import csv
import gzip
import glob
import argparse
import subprocess
from contextlib import closing

# Zoom level range for the POIs
MIN_ZOOMLEVEL = 10
MAX_ZOOMLEVEL = 14

def buildGeoJSON(geojsonFileName, inputFileNames, categoryIdFilter, qualityScoreThreshold, categoryMappings):
  with closing(gzip.open(geojsonFileName, mode='wt', encoding='utf-8')) as geojsonFile:
    geojsonFile.write('{ "type": "FeatureCollection", "features": [')
    featureCount = 0
    for inputFileName in inputFileNames:
      print('Converting %s' % inputFileName)
      with closing(gzip.open(inputFileName, mode='rt', encoding='utf-8')) as inputFile:
        reader = csv.DictReader(inputFile)
        for row in reader:
          id, categoryId, categoryName = row['id'], row['category_id'], row['category_name']
          nameOriginal = row['name_original']
          qualityRealityScore = float(row['quality_reality_score'])
          nameMulti = json.loads(row.get('name_multi_json', {}) or row.get('name_multi', {}) or row.get('to_json', {}))

          if qualityRealityScore < qualityScoreThreshold:
            continue
          if any([categoryId.startswith(filterPrefix) for filterPrefix in categoryIdFilter]):
            continue

          categoryClass = '_'.join(part.strip() for part in categoryName.strip().lower().replace("'", "").replace('&', ' ').replace(',', ' ').replace('-', '_').replace('/', '_').split() if part.strip())
          if categoryName in categoryMappings:
            mapping = categoryMappings[categoryName]
          else:
            mapping = { 'zoom': 14, 'category_class': categoryClass }
          if mapping['category_class'] != categoryClass:
            print('Warning: potential mismatch of category classes: %s <-> %s' % (mapping['category_class'], categoryClass))

          geojson = { 'type': 'Feature' }
          geojson['geometry'] = json.loads(row['geojson'])
          geojson['tippecanoe'] = { 'minzoom': min(MAX_ZOOMLEVEL, mapping['zoom']), 'maxzoom': MAX_ZOOMLEVEL }
          geojson['properties'] = { 'id': int(id[-12:], base=16) | (1 << 48), 'here_id': id, 'category_id': categoryId, 'name': nameOriginal, 'quality': int(qualityRealityScore * 100) }
          for key, val in mapping.items():
            if key != 'zoom':
              geojson['properties'][key] = val
          for lang, nameLang in nameMulti.items():
            if len(lang) == 2 and nameLang != nameOriginal:
              geojson['properties']['name:%s' % lang.lower()] = nameLang
          geojsonFile.write(',\n' if featureCount > 0 else '\n')
          geojsonFile.write('  ')
          geojsonFile.write(json.dumps(geojson))
          featureCount += 1
    geojsonFile.write('\n]}\n')

def buildMBTiles(outputFileName, geojsonFileName):
  if os.path.exists(outputFileName):
    os.remove(outputFileName)
  print('Building .mbtiles')
  result = subprocess.run(['tippecanoe', '--layer', 'poi', '--calculate-feature-density', '--use-attribute-for-id', 'id', '--minimum-zoom' ,'%d' % MIN_ZOOMLEVEL, '--maximum-zoom', '%d' % MAX_ZOOMLEVEL, '--drop-densest-as-needed', '--output', outputFileName, geojsonFileName])
  if result.returncode != 0:
    raise RuntimeError('Tippecanoe failed with return code %d' % result.returncode)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='input', help='input directory for .csv.gz files')
  parser.add_argument(dest='output', help='output name for .mbtiles file')
  parser.add_argument('--filter', dest='filter', default=None, help='name for .txt file containing categories to filter out')
  parser.add_argument('--quality', dest='quality', type=float, default=0.0, help='quality threshold level')
  args = parser.parse_args()

  if args.filter:
    with io.open(args.filter, mode='rt', encoding='utf-8') as f:
      categoryIdFilter = [row.strip() for row in f.readlines() if row.strip()]
  else:
    categoryIdFilter = []

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  with io.open("%s/parity_pois_mapping.json" % dataDir, mode='rt', encoding='utf-8') as f:
    categoryMappings = json.load(f)
  minCategoryZoom = min(mapping['zoom'] for mapping in categoryMappings.values())
  for key, mapping in list(categoryMappings.items()):
    if 'rank' not in mapping:
      mapping['rank'] = mapping['zoom'] - minCategoryZoom + 1
    categoryMappings[key] = mapping

  inputFileNames = glob.glob("%s/*.csv.gz" % args.input)
  geojsonFileName = '%s.geojson.gz' % args.output
  outputFileName = args.output

  try:
    buildGeoJSON(geojsonFileName, inputFileNames, categoryIdFilter, args.quality, categoryMappings)
  except Exception as e:
    if os.path.isfile(geojsonFileName):
      os.remove(geojsonFileName)
    print('Failed to convert packages: %s' % str(e), file=sys.stderr)
    raise

  try:
    buildMBTiles(outputFileName, geojsonFileName)
  except Exception as e:
    if os.path.isfile(outputFileName):
      os.remove(outputFileName)
    print('Failed to run tippecanoe: %s' % str(e), file=sys.stderr)
    raise

if __name__ == "__main__":
  main()
