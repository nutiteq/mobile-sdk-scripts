# Script for building tilemasks and packages.json template from .poly files

import io
import os
import glob
import json
import argparse
import utils.polygon2geojson as polygon2geojson
import utils.tilemask as tilemask

TILEMASK_SIZE_THRESHOLD = 512

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='input', help='Input directory for .poly files')
  parser.add_argument(dest='output', help='Output directory for packages.json.template files')
  args = parser.parse_args()

  os.makedirs(args.output, exist_ok=True)

  polyFilenames = glob.glob("%s/*.poly" % args.input)
  packages = []
  for polyFilename in polyFilenames:
    packageId = polyFilename.split("/")[-1][:-5].replace("_", "-").replace(' ', '_').lower()
    packageName = polyFilename.split("/")[-1][:-5].replace("_", "/")
    print("Converting %s" % packageId)

    geojson_filename = polyFilename + ".geojson"
    polygon2geojson.main(polyFilename, geojson_filename)
    for maxZoom in range(tilemask.DEFAULT_MAX_ZOOM - 4, tilemask.DEFAULT_MAX_ZOOM + 1):
      mask = tilemask.processPolygon(polyFilename + ".geojson", maxZoom)
      if len(mask) >= TILEMASK_SIZE_THRESHOLD:
        break

    packages.append(
      {
        'id': packageId,
        'version': 1,
        'tile_mask': str(mask, 'utf8'),
        'url': '', 
        'metainfo': { 'name_en': packageName },
        'size': 0
      }
    )

  with io.open('%s/packages.json.template' % args.output, 'wt', encoding='utf-8') as packagesFile:
    packagesContainer = {
      'packages': packages,
      'metainfo': {}
    }
    packagesFile.write(json.dumps(packagesContainer))

if __name__ == "__main__":
  main()
