# Script for building tilemasks and packages.json template from .poly files

import io
import os
import glob
import json
import argparse
import utils.polygon2geojson as polygon2geojson
import utils.tilemask as tilemask
import functools
import re
import geojson

TILEMASK_SIZE_THRESHOLD = 512

def bbox(coord_list):
     box = []
     for i in (0,1):
         res = sorted(coord_list, key=lambda x:x[i])
         box.append((res[0][i],res[-1][i]))
     return box


def read_polygon(polygon_filename):
  with open(polygon_filename) as f:
    return f.readlines()

def clean_polygon(polygon_data):
  coordinates = polygon_data[2:][:-2]
  coordinates = [re.split(r'[\s\t]+', item) for item in coordinates]
  coordinates = [list(filter(None, item)) for item in coordinates]
  coordinates = functools.reduce(lambda a,b: a[-1].pop(0) and a if len(a[-1]) == 1 and a[-1][0] == 'END' else a.append(['END']) or a if b[0].startswith('END') else a[-1].append(b) or a, [[[]]] + coordinates)
  coordinates = [[(float(item[0]), float(item[1])) for item in coordgroup] for coordgroup in coordinates]
  return coordinates


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

    polygon_data = read_polygon(polyFilename)
    coordinates = clean_polygon(polygon_data)
    poly=geojson.Polygon(coordinates)
    line = bbox(list(geojson.utils.coords(poly)))
    packages.append(
      {
        'id': packageId,
        'version': 1,
        'tile_mask': str(mask, 'utf8'),
        'bbox': (line[0][0], line[1][0], line[0][1],line[1][1]),
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
