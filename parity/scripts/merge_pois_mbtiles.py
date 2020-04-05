# Script for merging two planet .mbtiles files by replacing only POI layer (from the second file).
# The script also calculated POI ranks based on local POI density and POI priority.

import io
import os
import sys
import gzip
import zlib
import json
import sqlite3
import argparse
import multiprocessing
import concurrent.futures
import utils.tilemask as tilemask
import utils.mbvtpackage_pb2 as mbvtpackage_pb2
from contextlib import closing

# Maximum zoom level used in offline packages
MAX_ZOOMLEVEL = 14

# Number of worker processes to use
NUM_WORKERS = 8

def mergeTileData(tileData1, tileData2, classZoomTable):
  # Fast out - do we have to decode the tile at all?
  if tileData1 is not None and tileData2 is None:
    if tileData1.find(b'poi') == -1:
      return tileData1

  # Remove OSM POI layer from first tile
  tile1 = mbvtpackage_pb2.Tile()
  if tileData1 is not None:
    tile1.ParseFromString(tileData1)
    for index in range(0, len(tile1.layers)):
      if tile1.layers[index].name == 'poi':
        del tile1.layers[index]
        break

  # Find HERE POI layer from second tile and merge it into first tile
  if tileData2 is not None:
    tile2 = mbvtpackage_pb2.Tile()
    tile2.ParseFromString(tileData2)
    for layer in tile2.layers:
      if layer.name == 'poi':
        for i in range(0, len(layer.keys)):
          if layer.keys[i] == 'tippecanoe_feature_density':
            layer.keys[i] = 'density'
        tile1.layers.append(layer)
        break

  # Compress the resulting tile
  return tile1.SerializeToString()

def compressTile(tileData, zdict=None):
  if zdict is not None:
    compress = zlib.compressobj(9, zlib.DEFLATED, -15, 5, zlib.Z_DEFAULT_STRATEGY, zdict)
  else:
    compress = zlib.compressobj(9, zlib.DEFLATED, 31, 5, zlib.Z_DEFAULT_STRATEGY)
  deflated = compress.compress(tileData)
  deflated += compress.flush()
  return deflated

def decompressTile(tileData, zdict=None):
  if zdict is not None:
    decompress = zlib.decompressobj(-15, zdict)
  else:
    if tileData[0:2] != b'\x1f\x8b':
      return tileData
    decompress = zlib.decompressobj(47)
  inflated = decompress.decompress(tileData)
  inflated += decompress.flush()
  return inflated

def generateTiles(tileMask, minZoom, maxZoom, iterConfig=(0, 1)):
  if tileMask is not None:
    counter = 0
    for x, y, zoom in tilemask.tileMaskTiles(tileMask, maxZoom):
      if counter % iterConfig[1] == iterConfig[0]:
        yield (x, y, zoom)
      counter += 1
  else:
    counter = 0
    for zoom in range(minZoom, maxZoom + 1):
      for y in range(0, 2**zoom):
        for x in range(0, 2**zoom):
          if counter % iterConfig[1] == iterConfig[0]:
            yield (x, y, zoom)
          counter += 1

def mergeTiles(worldFileName, poiFileName, outputFileName, iterConfig, zcompress=True, tileMask=None, classZoomTable={}):
  if os.path.isfile(outputFileName):
    os.remove(outputFileName)

  # Open world input file, POI file
  with closing(sqlite3.connect('file:%s?mode=ro' % worldFileName, uri=True)) as worldDb:
    worldCursor = worldDb.cursor()
    with closing(sqlite3.connect('file:%s?mode=ro' % poiFileName, uri=True)) as poiDb:
      poiCursor = poiDb.cursor()
      with closing(sqlite3.connect(outputFileName)) as outputDb:
        outputDb.execute("PRAGMA locking_mode=EXCLUSIVE")
        outputDb.execute("PRAGMA synchronous=OFF")
        outputDb.execute("PRAGMA page_size=512")
        outputDb.execute("PRAGMA encoding='UTF-8'")

        # Create tiles table
        outputCursor = outputDb.cursor()
        outputCursor.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")

        # Copy encryption info (we assume all packages share this)
        worldCursor.execute("SELECT value FROM metadata WHERE name='nutikeysha1'")
        row = worldCursor.fetchone()
        if row:
          outputCursor.execute("INSERT INTO metadata(name, value) VALUES('nutikeysha1', ?)", (row[0],))

        # Process tiles
        for x, y, zoom in generateTiles(tileMask, 0, MAX_ZOOMLEVEL, iterConfig):
          worldCursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, y))
          row = worldCursor.fetchone()
          tileData1 = decompressTile(bytes(row[0])) if row else None
          poiCursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (zoom, x, y))
          row = poiCursor.fetchone()
          tileData2 = decompressTile(bytes(row[0])) if row else None
          if tileData1 or tileData2:
            mergedTileData = mergeTileData(tileData1, tileData2, classZoomTable)
            if zcompress:
              finalTileData = compressTile(mergedTileData)
            else:
              finalTileData = mergedTileData
            outputCursor.execute("INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(?, ?, ?, ?)", (zoom, x, y, finalTileData))

        # Close output file
        outputCursor.execute("CREATE UNIQUE INDEX tiles_index ON tiles (zoom_level, tile_column, tile_row)");
        outputDb.commit()
  return outputFileName

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='world', help='world .mbtiles file')
  parser.add_argument(dest='poi', help='poi .mbtiles file')
  parser.add_argument(dest='output', help='output .mbtiles file')
  parser.add_argument('--tilemask', dest='tilemask', default=None, help='optional tilemask')
  parser.add_argument('--compress', dest='compress', type=int, default=1, help='compress tiles')
  args = parser.parse_args()

  dataDir = '%s/../data' % os.path.realpath(os.path.dirname(__file__))
  classZoomTable = {}
  with io.open("%s/parity_pois_mapping.json" % dataDir, mode='rt', encoding='utf-8') as f:
    for categoryName, mapping in json.load(f).items():
      classZoomTable[mapping["category_class"]] = mapping["zoom"]

  if os.path.isfile(args.output):
    os.remove(args.output)

  numProcesses = NUM_WORKERS or multiprocessing.cpu_count()

  with concurrent.futures.ProcessPoolExecutor(max_workers=numProcesses) as executor:
    results = [executor.submit(mergeTiles, args.world, args.poi, '%s.%d.tmp' % (args.output, i), (i, numProcesses), args.compress, args.tilemask, classZoomTable) for i in range(0, numProcesses)]

  outputDb = sqlite3.connect(args.output)
  outputDb.execute("PRAGMA synchronous=OFF")
  outputDb.execute("PRAGMA page_size=512")
  outputDb.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
  outputDb.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('name', 'planet')")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('type', 'baselayer')")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('version', '1.0')")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('description', 'Nutiteq map package for planet')")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('format', 'mbvt')")
  outputDb.execute("INSERT INTO metadata(name, value) VALUES('schema', 'parity.streets')")
  for result in results:
    try:
      chunkFileName = result.result()
      outputDb.execute("ATTACH DATABASE ? AS chunk_db", (chunkFileName,))
      outputDb.execute("INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) SELECT zoom_level, tile_column, tile_row, tile_data FROM chunk_db.tiles")
      outputDb.execute("DETACH DATABASE chunk_db")
      os.remove(chunkFileName)
    except Exception as e:
      outputDb.close()
      if os.path.isfile(args.output):
        os.remove(args.output)
      raise
  outputDb.execute("CREATE UNIQUE INDEX tiles_index ON tiles (zoom_level, tile_column, tile_row)");
  outputDb.commit()
  outputDb.close()

if __name__ == "__main__":
  main()
