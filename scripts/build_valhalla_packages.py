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

# Zoom level/precision for tilemasks
TILEMASK_ZOOM = 10

# Generic projection values
VALHALLA_BOUNDS = ((-180, -90), (180, 90))
VALHALLA_TILESIZES = [4.0, 1.0, 0.25]
MERCATOR_BOUNDS = ((-6378137 * math.pi, -6378137 * math.pi), (6378137 * math.pi, 6378137 * math.pi))

class PackageTileMask(object):
  def __init__(self, tileMaskStr):
    self.data = self._decodeTileMask(tileMaskStr)
    self.rootNode = self._buildTileNode(list(self.data), (0, 0, 0))

  def contains(self, tile):
    node = self._findTileNode(tile)
    if node is None:
      return False
    return node["inside"]

  def getTiles(self, maxZoom=None):
    tiles = []
    if self.data != []:
      self._buildTiles(list(self.data), (0, 0, 0), maxZoom, tiles)
    return tiles

  def _decodeTileMask(self, tileMaskStr):
    str = [c for c in base64.b64decode(tileMaskStr)]
    data = []
    for i in range(len(str) * 8):
      val = (str[i // 8] >> (7 - i % 8)) & 1
      data.append(val)
    return data

  def _buildTileNode(self, data, tile):
    (zoom, x, y) = tile
    subtiles = data.pop(0)
    inside = data.pop(0)
    node = { "tile" : tile, "inside": inside, "subtiles": [] }
    if subtiles:
      for dy in range(0, 2):
        for dx in range(0, 2):
          node["subtiles"].append(self._buildTileNode(data, (zoom + 1, x * 2 + dx, y * 2 + dy)))
    return node

  def _findTileNode(self, tile):
    (zoom, x, y) = tile
    if zoom == 0:
      return self.rootNode if tile == (0, 0, 0) else None

    parentNode = self._findTileNode((zoom - 1, x >> 1, y >> 1))
    if parentNode:
      for node in parentNode["subtiles"]:
        if node["tile"] == tile:
          return node
      if parentNode["inside"]:
        return parentNode
    return None

  def _buildTiles(self, data, tile, maxZoom, tiles):
    (zoom, x, y) = tile
    submask = data.pop(0)
    inside = data.pop(0)
    if inside:
      tiles.append(tile)
    if submask:
      for dy in range(0, 2):
        for dx in range(0, 2):
          self._buildTiles(data, (zoom + 1, x * 2 + dx, y * 2 + dy), maxZoom, tiles)
    elif maxZoom is not None and inside:
      for dy in range(0, 2):
        for dx in range(0, 2):
          self._buildAllTiles((zoom + 1, x * 2 + dx, y * 2 + dy), maxZoom, tiles)

  def _buildAllTiles(self, tile, maxZoom, tiles):
    (zoom, x, y) = tile
    if zoom > maxZoom:
      return
    tiles.append(tile)
    for dy in range(0, 2):
      for dx in range(0, 2):
        self._buildAllTiles((zoom + 1, x * 2 + dx, y * 2 + dy), maxZoom, tiles)

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

def _calculateValhallaTiles(mTile, vZoom, epsg3857, epsg4326):
  mTileSize = (MERCATOR_BOUNDS[1][0] - MERCATOR_BOUNDS[0][0]) / (1 << mTile[2])
  vTileSize = VALHALLA_TILESIZES[vZoom]
  mX0, mY0 = mTile[0] * mTileSize + MERCATOR_BOUNDS[0][0], mTile[1] * mTileSize + MERCATOR_BOUNDS[0][1]
  mX1, mY1 = mX0 + mTileSize, mY0 + mTileSize
  vX0, vY0 = pyproj.transform(epsg3857, epsg4326, mX0, mY0)
  vX1, vY1 = pyproj.transform(epsg3857, epsg4326, mX1, mY1)
  vTile0 = (vX0 - VALHALLA_BOUNDS[0][0]) / vTileSize, (vY0 - VALHALLA_BOUNDS[0][1]) / vTileSize
  vTile1 = (vX1 - VALHALLA_BOUNDS[0][0]) / vTileSize, (vY1 - VALHALLA_BOUNDS[0][1]) / vTileSize
  vTiles = []
  for y in range(int(math.floor(vTile0[1])), int(math.ceil(vTile1[1]))):
    for x in range(int(math.floor(vTile0[0])), int(math.ceil(vTile1[0]))):
      vTiles.append((x, y, vZoom))
  return vTiles

def calculateValhallaTilesFromTileMask(tileMask):
  vTiles = set()
  mTiles = [(x, y, zoom) for zoom, x, y in PackageTileMask(tileMask).getTiles(TILEMASK_ZOOM)]
  epsg3857 = pyproj.Proj(init='EPSG:3857')
  epsg4326 = pyproj.Proj(init='EPSG:4326')
  for mTile in mTiles:
    if mTile[2] < TILEMASK_ZOOM:
      continue
    for vZoom, vTileSize in enumerate(VALHALLA_TILESIZES):
      for vTile in _calculateValhallaTiles(mTile, vZoom, epsg3857, epsg4326):
        vTiles.add(vTile)
  return sorted(list(vTiles))

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

def extractTiles(packageId, tileMask, outputFileName, valhallaTileDir, zdict=None):
  if os.path.exists(outputFileName):
    os.remove(outputFileName)

  with closing(sqlite3.connect(outputFileName)) as outputDb:
    outputDb.execute("PRAGMA locking_mode=EXCLUSIVE")
    outputDb.execute("PRAGMA synchronous=OFF")
    outputDb.execute("PRAGMA page_size=512")
    outputDb.execute("PRAGMA encoding='UTF-8'")

    cursor = outputDb.cursor();
    cursor.execute("CREATE TABLE metadata (name TEXT, value TEXT)");
    cursor.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)");
    cursor.execute("INSERT INTO metadata(name, value) VALUES('name', ?)", (packageId,))
    cursor.execute("INSERT INTO metadata(name, value) VALUES('type', 'routing')")
    cursor.execute("INSERT INTO metadata(name, value) VALUES('version', '1.0')")
    cursor.execute("INSERT INTO metadata(name, value) VALUES('description', 'Nutiteq Valhalla routing package for ' || ?)", (packageId,))
    cursor.execute("INSERT INTO metadata(name, value) VALUES('format', 'gph')")
    if zdict is not None:
      cursor.execute("INSERT INTO metadata(name, value) VALUES('shared_zlib_dict', ?)", (bytes(zdict),))

    vTiles = calculateValhallaTilesFromTileMask(tileMask)
    for vTile in vTiles:
      file = os.path.join(valhallaTileDir, valhallaTilePath(vTile))
      if os.path.isfile(file):
        with closing(io.open(file, 'rb')) as sourceFile:
          compressedData = compressTile(sourceFile.read(), zdict)
          cursor.execute("INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(?, ?, ?, ?)", (vTile[2], vTile[0], vTile[1], bytes(compressedData)));
      else:
        print('Warning: File %s does not exist!' % file)

    cursor.execute("CREATE UNIQUE INDEX tiles_index ON tiles (zoom_level, tile_column, tile_row)");
    cursor.close()
    outputDb.commit()

  with closing(sqlite3.connect(outputFileName)) as outputDb:
    outputDb.execute("VACUUM")

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
    extractTiles(package['id'], package['tile_mask'], outputFileName, tilesDir, zdict)
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
