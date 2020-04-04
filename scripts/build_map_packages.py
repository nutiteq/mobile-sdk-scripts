# Script for extracting vector tiles (from OMT planet database) into .mbtiles sqlite
# databases.

import io
import os
import sys
import json
import gzip
import zlib
import base64
import sqlite3
import argparse
import concurrent.futures
from contextlib import closing
import utils.mbvtpackage_pb2 as mbvtpackage_pb2

# Package URL template
DEFAULT_PACKAGE_URL_TEMPLATE = 'FULL_PACKAGE_URL/{version}/{id}.mbtiles?appToken={{key}}'

# Default package version
DEFAULT_PACKAGE_VERSION = 1

# Maximum zoom level used in offline packages
MAX_ZOOMLEVEL = 14

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

def decodeCoordinates(data, scale):
  vertices = []
  verticesList = []
  cx = 0
  cy = 0
  cmd = 0
  length = 0
  i = 0
  while i < len(data):
    if length == 0:
      cmdLength = data[i]
      i += 1
      length = cmdLength >> 3
      cmd = cmdLength & 7
      if length == 0:
        continue

    length -= 1
    if cmd == 1 or cmd == 2:
      if cmd == 1:
        if vertices:
          verticesList.append(vertices)
          vertices = []
      dx = data[i]
      i += 1
      dy = data[i]
      i += 1
      dx = ((dx >> 1) ^ (-(dx & 1)))
      dy = ((dy >> 1) ^ (-(dy & 1)))
      cx += dx
      cy += dy
      vertices.append((float(cx) / scale, float(cy) / scale))
    elif cmd == 7:
      pass
  if vertices:
    verticesList.append(vertices)
  return verticesList

def clippedVertices(vertices):
  return [(max(min(x, 1.0), 0.0), max(min(y, 1.0), 0.0)) for x, y in vertices]

def clippedVertexPairs(vertices):
  if len(vertices) < 3:
    return []
  vertices = clippedVertices(vertices)
  vertexPairs = []
  for i in range(0, len(vertices)):
    j = (i + 1) % len(vertices)
    if vertices[i] != vertices[j]:
      vertexPairs.append((vertices[i], vertices[j]))
  return vertexPairs

def classifyVertexPair(vertexPair):
  v0, v1 = vertexPair
  if v0[0] in (0.0, 1.0) and v0[1] in (0.0, 1.0) and v0[0] == v1[0] and v0[1] == 1.0 - v1[1]:
    return 'x%g' % v0[0]
  if v0[1] in (0.0, 1.0) and v0[0] in (0.0, 1.0) and v0[1] == v1[1] and v0[0] == 1.0 - v1[0]:
    return 'y%g' % v0[1]
  return None

def isDegenerateRing(vertices):
  classes = [classifyVertexPair(vertexPair) for vertexPair in clippedVertexPairs(vertices)]
  if None in classes:
    return False
  if len(set(classes)) < 3:
    return True
  ring = []
  for v0, v1 in clippedVertexPairs(vertices):
    if len(ring) < 1:
      ring.append(v0)
      ring.append(v1)
    else:
      if len(ring) > 1 and ring[-2] == v1:
        del ring[-1]
      else:
        ring.append(v1)
  return len(ring) < 3

def isFullRing(vertices):
  classes = [classifyVertexPair(vertexPair) for vertexPair in clippedVertexPairs(vertices)]
  if None in classes:
    return False
  order = "_".join(classes + classes)
  if not ("x0_y0_x1_y1" in order or "x0_y1_x1_y0" in order):
    return False
  ring = []
  for v0, v1 in clippedVertexPairs(vertices):
    if len(ring) < 1:
      ring.append(v0)
      ring.append(v1)
    else:
      if len(ring) > 1 and ring[-2] == v1:
        del ring[-1]
      else:
        ring.append(v1)
  return len(ring) > 3

def isFullPolygon(verticesList):
  if len(verticesList) < 1:
    return False
  return isFullRing(verticesList[0]) and all([isDegenerateRing(vertices) for vertices in verticesList[1:]])

def isEmptyPolygon(verticesList):
  if len(verticesList) < 1:
    return True
  return all([isDegenerateRing(vertices) for vertices in verticesList])

def optimizeTile(tileData):
  # Decompress/parse tile
  tile = mbvtpackage_pb2.Tile()
  tile.ParseFromString(tileData)

  # Optimize polygons, detect tile covering/empty polygons.
  # This is important as Mapnik Vector Tile renderer creates redundant vertices
  # and equivalent polygons have different encodings.
  for layer in tile.layers:
    index = 0
    while index < len(layer.features):
      feature = layer.features[index]
      if feature.type == 3:
        verticesList = decodeCoordinates(feature.geometry, layer.extent)
        if isEmptyPolygon(verticesList):
          del layer.features[index]
          continue
        if isFullPolygon(verticesList):
          if layer.extent == 4096: # feature encoding assumes this extent
            del feature.geometry[:]
            feature.geometry.extend([9, 255, 8448, 34, 0, 8703, 0, 0, 8704, 0, 0, 8704, 15])
      index += 1

  # Remove empty layers
  index = 0
  while index < len(tile.layers):
    if len(tile.layers[index].features) == 0:
      del tile.layers[index]
      continue
    index += 1

  # Compress tile
  tileData = tile.SerializeToString()
  return tileData

def compressTile(tileData, zdict=None):
  if zdict is not None:
    compress = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, zdict)
  else:
    compress = zlib.compressobj(9, zlib.DEFLATED, 31, 9, zlib.Z_DEFAULT_STRATEGY)
  deflated = compress.compress(tileData)
  deflated += compress.flush()
  return deflated

def decompressTile(tileData, zdict=None):
  if zdict is not None:
    decompress = zlib.decompressobj(-15, zdict)
  else:
    decompress = zlib.decompressobj(47)
  inflated = decompress.decompress(tileData)
  inflated += decompress.flush()
  return inflated

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

def extractTiles(packageId, tileMask, worldFileName, outputFileName, maxZoom=14, zdict=None):
  # Decode tilemask, create full list of tiles up to specified zoom level
  tiles = PackageTileMask(tileMask).getTiles(maxZoom)
  tiles.reverse() # reverse tiles, for more optimal hit

  # Open input file
  with closing(sqlite3.connect('file:%s?mode=ro' % worldFileName, uri=True)) as packageDb:
    packageCursor = packageDb.cursor()
    
    # Open output file and prepare database
    if os.path.exists(outputFileName):
      os.remove(outputFileName)

    with closing(sqlite3.connect(outputFileName)) as outputDb:
      outputDb.execute("PRAGMA locking_mode=EXCLUSIVE")
      outputDb.execute("PRAGMA synchronous=OFF")
      outputDb.execute("PRAGMA page_size=512")
      outputDb.execute("PRAGMA encoding='UTF-8'")

      outputCursor = outputDb.cursor()
      outputCursor.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
      outputCursor.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('name', ?)", (packageId,))
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('type', 'baselayer')")
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('version', '1.0')")
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('description', 'Nutiteq map package for ' || ?)", (packageId,))
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('format', 'mbvt')")
      outputCursor.execute("INSERT INTO metadata(name, value) VALUES('schema', 'carto.streets')")
      if zdict is not None:
        outputCursor.execute("INSERT INTO metadata(name, value) VALUES('shared_zlib_dict', ?)", (bytes(zdict),))

      # Copy encryption info (we assume all packages share this)
      packageCursor.execute("SELECT value FROM metadata WHERE name='nutikeysha1'")
      row = packageCursor.fetchone()
      if row:
        outputCursor.execute("INSERT INTO metadata(name, value) VALUES('nutikeysha1', ?)", (row[0],))

      i = 0
      firstTile = True
      missingTiles = 0
      # Process tiles
      prevTileData = None
      while i < len(tiles):
        zoom, x, y = tiles[i]
        packageCursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, y))
        row = packageCursor.fetchone()
        if not row:
          missingTiles += 1
        else:
          tileData = bytes(row[0])
          if tileData != prevTileData:
            if tileData[0:2] == b'\x1f\x8b':
              uncompressedTileData = decompressTile(tileData)
            else:
              uncompressedTileData = tileData
            optimizedTileData = optimizeTile(uncompressedTileData)
            compressedTileData = compressTile(optimizedTileData, zdict)
          outputCursor.execute("INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(?, ?, ?, ?)", (zoom, x, y, compressedTileData))
          prevTileData = tileData
        i += 1

      # Close output file
      outputCursor.execute("CREATE UNIQUE INDEX tiles_index ON tiles (zoom_level, tile_column, tile_row)");
      outputCursor.close()
      outputDb.commit()

  # Vacuum the database
  with closing(sqlite3.connect(outputFileName)) as outputDb:
    outputDb.execute("VACUUM")

def optimizeTiles(outputFileName):
  # Drop tiles that are not needed
  with closing(sqlite3.connect(outputFileName)) as outputDb:
    outputDb.execute("PRAGMA locking_mode=EXCLUSIVE")
    outputDb.execute("PRAGMA synchronous=OFF")
    outputDb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tiles_index ON tiles (zoom_level, tile_column, tile_row)");

    # Harvest tiles
    cursor1 = outputDb.cursor()
    cursor2 = outputDb.cursor()
    for zoom in range(MAX_ZOOMLEVEL, 0, -1):
      # Find tiles at specified zoom levels equal to their parent tiles
      cursor1.execute("SELECT t.tile_column, t.tile_row FROM tiles t, tiles s WHERE t.zoom_level=? AND s.zoom_level=?-1 AND t.tile_column/2=s.tile_column AND t.tile_row/2=s.tile_row AND t.tile_data=s.tile_data", (zoom, zoom))
      for row in cursor1.fetchall():
        # Now check that there are no child tiles. In that case the tile can be deleted
        x, y = row
        cursor2.execute("SELECT zoom_level FROM tiles WHERE zoom_level=?+1 AND (tile_column BETWEEN ?*2 AND ?*2+1) AND (tile_row BETWEEN ?*2 AND ?*2+1)", (zoom, x, x, y, y))
        if not cursor2.fetchone():
          cursor2.execute("DELETE FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, y))
    cursor2.close()
    cursor1.close()
    outputDb.commit()

  # Vacuum
  with closing(sqlite3.connect(outputFileName)) as outputDb:
    outputDb.execute("VACUUM")

def processPackage(package, outputDir, inputFileName, zdictDir=None):
  outputFileName = '%s/%s.mbtiles' % (outputDir, package['id'])
  if os.path.exists(outputFileName):
    if not os.path.exists(outputFileName + "-journal"):
      return outputFileName
    os.remove(outputFileName)
    os.remove(outputFileName + "-journal")

  print('Processing %s' % package['id'])
  try:
    zdict = loadZDict(package['id'], zdictDir)
    extractTiles(package['id'], package['tile_mask'], inputFileName, outputFileName, MAX_ZOOMLEVEL, zdict)
    optimizeTiles(outputFileName)
  except:
    if os.path.isfile(outputFileName):
      os.remove(outputFileName)
    raise
  return outputFileName

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='template', help='template for packages.json')
  parser.add_argument(dest='input', help='planet .mbtiles file')
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
