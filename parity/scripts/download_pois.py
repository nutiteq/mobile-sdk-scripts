# Script for downloading per-country POI information using CARTO COPYTO/FROM API.

import os
import io
import json
import subprocess
import argparse
import urllib.parse

# API key needed to access the 'here_pois_final' table
CARTO_API_KEY = "mj-KSaH6ablBnOyuKcEElg"

# Minimum POI quality to download
DEFAULT_POI_QUALITY = 0.3

def downloadCountryData(outputDir, countryCode, qualityThreshold):
  outputFileName = "%s/%s.csv" % (outputDir, countryCode)
  if os.path.exists(outputFileName + ".gz"):
    return

  print('Downloading %s' % countryCode)
  query = "SELECT id,category_id,category_name,quality_reality_score,name_original,To_JSON(name_multi) AS name_multi_json,num_house,address_original,ST_AsGeoJSON(the_geom) AS geojson FROM here_pois_final WHERE country_code='%s' AND quality_reality_score>=%g" % (countryCode, qualityThreshold)
  params = { 'q': 'COPY (%s) TO stdout WITH(FORMAT csv,HEADER true)' % query, 'api_key': CARTO_API_KEY }
  cmdLine = ["curl", "--output", outputFileName, "--compressed", "https://parity-admin.carto.com/api/v2/sql/copyto?%s" % urllib.parse.urlencode(params)]
  result = subprocess.run(cmdLine)
  if result.returncode != 0:
    raise RuntimeError('Failed to download %s, return code %d' % (countryCode, result.returncode))

  print('GZipping %s' % countryCode)
  cmdLine = ["gzip", outputFileName]
  result = subprocess.run(cmdLine)
  if result.returncode != 0:
    raise RuntimeError('Failed to gzip %s, return code %d' % (countryCode, result.returncode))

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='output', help='output directory for .csv.gz files')
  parser.add_argument('--country', dest='country', default=None, help='ISO3 country codes to download')
  parser.add_argument('--quality', dest='quality', type=float, default=DEFAULT_POI_QUALITY, help='minimum quality value')
  args = parser.parse_args()

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  if args.country:
    countryCodes = args.country.split(',')
  else:
    with io.open('%s/iso3_country_codes.json' % dataDir, 'rt', encoding='utf-8') as f:
      countryCodes = sorted([record["alpha-3"] for record in json.load(f)])

  os.makedirs(args.output, exist_ok=True)
  for countryCode in countryCodes:
    try:
      downloadCountryData(args.output, countryCode, args.quality)
    except Exception as e:
      print('Failed to download country %s: %s' % (countryCode, str(e)), file=sys.stderr)
      raise

if __name__ == "__main__":
  main()
