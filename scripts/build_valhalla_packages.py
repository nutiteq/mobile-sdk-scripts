# Script for extracting valhalla tiles (from Valhalla tile directory structure) into .vtiles sqlite
# databases.

import io
import os
import sys
import json
import gzip
import zlib
import base64
import pyproj
import math
import sqlite3
import argparse
import concurrent.futures
from contextlib import closing

# Package URL template
DEFAULT_PACKAGE_URL_TEMPLATE = 'FULL_PACKAGE_URL/{version}/{id}.vtiles?appToken={{key}}'

# Default package version
DEFAULT_PACKAGE_VERSION = 1

# Generic projection values
VALHALLA_BOUNDS = ((-180, -90), (180, 90))
VALHALLA_TILESIZES = [4.0, 1.0, 0.25]

def valhallaTilePath(vTile):
  vTileSize = VALHALLA_TILESIZES[vTile[2]]
  r = int((VALHALLA_BOUNDS[1][0] - VALHALLA_BOUNDS[0][0]) / vTileSize)
  id = vTile[1] * r + vTile[0]
  splitId = []
  for i in range(0, max(1, vTile[2]) + 1):
    splitId = ['%03d' % (id % 1000)] + splitId
    id /= 1000
  splitId = [str(vTile[2])] + splitId
  return '/'.join(splitId) + '.gph'

def compressTile(tileData, zdict=None):
  if zdict is not None:
    compress = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, zdict)
  else:
    compress = zlib.compressobj(9, zlib.DEFLATED, 31, 9, zlib.Z_DEFAULT_STRATEGY)
  deflated = compress.compress(tileData)
  deflated += compress.flush()
  return deflated

def loadZDict(packageId, zdictDir):
  if zdictDir is None:
    return None
  parts = packageId.split('-')
  fileNames = ["%s/%s.zdict" % (zdictDir, '-'.join(parts[:n])) for n in range(len(parts), 0, -1)]
  for fileName in fileNames:
    if os.path.exists(fileName):
      with closing(io.open(fileName, 'rb')) as dictFile:
        return dictFile.read()
  print('Warning: Could not find dictionary for package %s!' % packageId)
  return None


valhalla_tiles = [{'level': 2, 'size': 0.25}, {'level': 1, 'size': 1.0}, {'level': 0, 'size': 4.0}]
LEVEL_BITS = 3
TILE_INDEX_BITS = 22
ID_INDEX_BITS = 21
LEVEL_MASK = (2**LEVEL_BITS) - 1
TILE_INDEX_MASK = (2**TILE_INDEX_BITS) - 1
ID_INDEX_MASK = (2**ID_INDEX_BITS) - 1
INVALID_ID = (ID_INDEX_MASK << (TILE_INDEX_BITS + LEVEL_BITS)) | (TILE_INDEX_MASK << LEVEL_BITS) | LEVEL_MASK

def get_tile_level(id):
  return id & LEVEL_MASK

def get_tile_index(id):
  return (id >> LEVEL_BITS) & TILE_INDEX_MASK

def get_index(id):
  return (id >> (LEVEL_BITS + TILE_INDEX_BITS)) & ID_INDEX_MASK

def tiles_for_bounding_box(left, bottom, right, top):
  #if this is crossing the anti meridian split it up and combine
  if left > right:
    east = tiles_for_bounding_box(left, bottom, 180.0, top)
    west = tiles_for_bounding_box(-180.0, bottom, right, top)
    return east + west
  #move these so we can compute percentages
  left += 180
  right += 180
  bottom += 90
  top += 90
  tiles = []
  #for each size of tile
  for tile_set in valhalla_tiles:
    #for each column
    for x in range(int(left/tile_set['size']), int(right/tile_set['size']) + 1):
      #for each row
      for y in range(int(bottom/tile_set['size']), int(top/tile_set['size']) + 1):
        #give back the level and the tile index
        # value = int(y * (360.0/tile_set['size']) + x)
        # tiles.append((tile_set['level'], int(value / 1000), value - int(value / 1000)*1000))
        tiles.append((x, y, tile_set['level']))
  return tiles

def get_tile_id(tile_level, lat, lon):
  level = filter(lambda x: x['level'] == tile_level, valhalla_tiles)[0]
  width = int(360 / level['size'])
  return int((lat + 90) / level['size']) * width + int((lon + 180 ) / level['size'])

def get_ll(id):
  tile_level = get_tile_level(id)
  tile_index = get_tile_index(id)
  level = filter(lambda x: x['level'] == tile_level, valhalla_tiles)[0]
  width = int(360 / level['size'])
  height = int(180 / level['size'])
  return int(tile_index / width) * level['size'] - 90, (tile_index % width) * level['size'] - 180
  

def extractTiles(packageId, bbox, outputFileName, valhallaTileDir, zdict=None):
  if os.path.exists(outputFileName):
    os.remove(outputFileName)
  with closing(sqlite3.connect(outputFileName)) as outputDb:
    cursor = outputDb.cursor();
    cursor.execute("PRAGMA synchronous=OFF")
    cursor.execute("PRAGMA page_size=512")
    cursor.execute("CREATE TABLE metadata (name TEXT, value TEXT)");
    cursor.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)");
    cursor.execute("INSERT INTO metadata(name, value) VALUES('name', ?)", (packageId,))
    cursor.execute("INSERT INTO metadata(name, value) VALUES('type', 'routing')")
    cursor.execute("INSERT INTO metadata(name, value) VALUES('version', '1.0')")
    cursor.execute("INSERT INTO metadata(name, value) VALUES('description', 'Nutiteq Valhalla routing package for ' || ?)", (packageId,))
    cursor.execute("INSERT INTO metadata(name, value) VALUES('format', 'gph')")
    if zdict is not None:
      cursor.execute("INSERT INTO metadata(name, value) VALUES('shared_zlib_dict', ?)", (bytes(zdict),))

    vTiles = tiles_for_bounding_box(bbox[0], bbox[1], bbox[2], bbox[3])
    for vTile in vTiles:
      file = os.path.join(valhallaTileDir, valhallaTilePath(vTile))
      if os.path.isfile(file):
        with closing(io.open(file, 'rb')) as sourceFile:
          compressedData = compressTile(sourceFile.read(), zdict)
          cursor.execute("INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(?, ?, ?, ?)", (vTile[2], vTile[0], vTile[1], bytes(compressedData)));
      else:
        print('Warning: File %s does not exist!' % file)

    cursor.execute("CREATE UNIQUE INDEX tiles_index ON tiles (zoom_level, tile_column, tile_row)");
    # cursor.execute("VACUUM")
    outputDb.commit()

def processPackage(package, outputDir, tilesDir, zdictDir=None):
  outputFileName = '%s/%s.vtiles' % (outputDir, package['id'])
  if os.path.exists(outputFileName):
    if not os.path.exists(outputFileName + "-journal"):
      return outputFileName
    os.remove(outputFileName)
    os.remove(outputFileName + "-journal")

  print('Processing %s' % package['id'])
  try:
    zdict = loadZDict(package['id'], zdictDir)
    extractTiles(package['id'], package['bbox'], outputFileName, tilesDir, zdict)
  except:
    if os.path.isfile(outputFileName):
      os.remove(outputFileName)
    raise
  return outputFileName
    
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='input', help='directory for Valhalla tiles')
  parser.add_argument(dest='output', help='output directory for packages')
  parser.add_argument('--packages', dest='packages', default=None, help='package filter (comma seperated)')
  parser.add_argument('--version', dest='version', default=DEFAULT_PACKAGE_VERSION, type=int, help='package version')
  parser.add_argument('--url_template', dest='url_template', default=DEFAULT_PACKAGE_URL_TEMPLATE, help='package URL template')
  parser.add_argument('--zdict', dest='zdict', default=None, help='directory for package .zdict files')
  args = parser.parse_args()

  with io.open(args.template, 'rt', encoding='utf-8') as packagesFile:
    packagesTemplate = json.loads(packagesFile.read())
  packagesFilter = args.packages.split(',') if args.packages is not None else None
  packagesList = [package for package in packagesTemplate['packages'] if packagesFilter is None or package['id'] in packagesFilter]

  if args.zdict is not None:
    if not os.path.isdir(args.zdict):
      print('ZDict directory does not exist', file=sys.stderr)
      sys.exit(-1)

  os.makedirs(args.output, exist_ok=True)
  with concurrent.futures.ProcessPoolExecutor() as executor:
    results = { package['id']: executor.submit(processPackage, package, args.output, args.input, args.zdict) for package in packagesList }

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
