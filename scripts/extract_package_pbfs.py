# Extract individual package .pbf files using Osmium extract command.

import os
import io
import sys
import json
import argparse
import pyproj
import tempfile
import subprocess
import shapely.geometry
import utils.tilemask as tilemask
from contextlib import closing

PACKAGE_BATCH_SIZE = 8 # optimal assuming 32GB of RAM

def calculateExtract(package):
  geom = tilemask.tileMaskPolygon(package['tile_mask'])

  wgs84 = pyproj.Proj(init='epsg:4326')
  epsg3857 = pyproj.Proj(init='epsg:3857')

  if isinstance(geom, shapely.geometry.Polygon):
    geom = shapely.geometry.MultiPolygon([geom])

  ringsList = []
  for poly in geom.geoms:
    rings = []
    rings.append([pyproj.transform(epsg3857, wgs84, *p) for p in poly.exterior.coords])
    for interior in poly.interiors:
      rings.append([pyproj.transform(epsg3857, wgs84, *p) for p in interior.coords])
    ringsList.append(rings)
  return { 'output': '%s.osm.pbf' % package['id'], 'description': package['id'], 'multipolygon': ringsList }

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='planet', help='planet file (.osm.pbf or .osm.xml)')
  parser.add_argument(dest='output', help='output directory for extracts')
  parser.add_argument('--packages', dest='packages', default=None, help='package filter (comma seperated)')
  args = parser.parse_args()

  with io.open(args.template, 'rt', encoding='utf-8') as packagesFile:
    packagesTemplate = json.loads(packagesFile.read())
  packagesFilter = args.packages.split(',') if args.packages is not None else None
  packagesList = [package for package in packagesTemplate['packages'] if packagesFilter is None or package['id'] in packagesFilter]

  os.makedirs(args.output, exist_ok=True)
  for packageBatch in [packagesList[n:n+PACKAGE_BATCH_SIZE] for n in range(0, len(packagesList), PACKAGE_BATCH_SIZE)]:
    config = { 'extracts': [calculateExtract(package) for package in packageBatch], 'directory': args.output }
    tempFile = tempfile.NamedTemporaryFile('wt', encoding='utf-8', suffix='.json', delete=False)
    try:
      tempFile.write(json.dumps(config))
      tempFile.close()

      print('Importing packages %s' % ', '.join([package['id'] for package in packageBatch]))
      cmdLine = ['osmium', 'extract', '-c', tempFile.name, '--overwrite', args.planet]
      if subprocess.call(cmdLine) != 0:
        raise RuntimeError("Failed to import packages: command line %s" % cmdLine)
      os.remove(tempFile.name)
    except:
      if os.path.isfile(tempFile.name):
        os.remove(tempFile.name)
      raise
 
if __name__ == "__main__":
  main()
