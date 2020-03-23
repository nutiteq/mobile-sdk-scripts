# Script for building shared dictionaries for offline packages.
# The script uses zstd for building the dictionary. This dictionary can be reused with zlib.

import io
import os
import sys
import glob
import sqlite3
import argparse
import gzip
import zstd
import concurrent.futures

DEFAULT_DICT_SIZE = 32768

def decompressData(data):
  compressedfile = io.BytesIO(data)
  uncompressedfile = gzip.GzipFile(fileobj=compressedfile)
  data = uncompressedfile.read()
  uncompressedfile.close()
  compressedfile.close()
  return data

def processFile(inputFileName, outputDir, dictSize):
  print('Processing %s' % inputFileName)

  # Check that the file is not encrypted
  inputConn = sqlite3.connect(inputFileName)
  inputConn.isolation_level = None
  cursor = inputConn.cursor()
  cursor.execute("SELECT value FROM metadata WHERE name='shared_zlib_dict'")
  if cursor.fetchone():
    print('Dictionary already applied')
    inputConn.close()
    return

  # Load sample of tiles
  cursor2 = inputConn.cursor()
  cursor2.execute("SELECT SUM(LENGTH(tile_data)) FROM tiles")
  size = cursor2.fetchone()[0]
  skip = int(size / (50 * 1024 * 1024))
  cursor2.execute("SELECT tile_column, tile_row, zoom_level, tile_data FROM tiles")
  tiles = []
  count = 0
  for row in cursor2:
    count += 1
    if skip > 0:
      if count % skip != 0:
         continue
    x, y, zoom, tileData = row
    tiles.append(decompressData(tileData))
  inputConn.close()

  # Do the training
  zdict = zstd.train_dictionary(dictSize, tiles)
  outputFileName = "%s/%s.zdict" % (outputDir, os.path.splitext(os.path.basename(inputFileName))[0])
  with io.open(outputFileName, 'wb') as f:
    f.write(zdict.as_bytes())
  return outputFileName

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='input', help='mbTiles (or vTiles) file patterns')
  parser.add_argument(dest='output', help='directory for .zdict files')
  parser.add_argument('--dict_size', dest='dictSize', type=int, default=DEFAULT_DICT_SIZE, help='dictionary size in bytes')
  args = parser.parse_args()

  os.makedirs(args.output, exist_ok=True)
  fileNames = glob.glob(args.input)
  with concurrent.futures.ProcessPoolExecutor() as executor:
    results = { fileName: executor.submit(processFile, fileName, args.output, args.dictSize) for fileName in fileNames }
    for fileName, result in results.items():
      try:
        result.result()
      except Exception as e:
        print('File %s failed: %s' % (fileName, str(e)), file=sys.stderr)

if __name__ == "__main__":
  main()
