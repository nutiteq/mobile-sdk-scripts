import math
import shapely.ops
import shapely.geometry

EARTH_R = 6378137
DEG_TO_RAD = 0.017453292519943295
RAD_TO_DEG = 57.295779513082323
MAX_LEVEL = 18
LEVEL_BITS = 5

def wm2wgs(x, y):
  num3 = x / EARTH_R
  num4 = num3 * RAD_TO_DEG
  num5 = math.floor((num4 + 180.0) / 360.0)
  num6 = num4 - (num5 * 360.0)
  num7 = 0.5 * math.pi - (2.0 * math.atan(math.exp(-y / EARTH_R)))
  return (num6, num7 * RAD_TO_DEG)

def wgs2wm(lng, lat):
  num = lng * DEG_TO_RAD
  x = EARTH_R * num
  a = lat * DEG_TO_RAD
  y = 0.5 * EARTH_R * math.log((1.0 + math.sin(a)) / (1.0 - math.sin(a)))
  return (x, y)

def calculatePointTile(x, y, zoom):
  d = EARTH_R * math.pi
  s = 2 * d / (1 << zoom)
  return (zoom, int(math.floor((x + d) / s)), int(math.floor((y + d) / s)))

def calculateTileBounds(zoom, xt, yt):
  d = EARTH_R * math.pi
  s = 2 * d / (1 << zoom)
  return (xt * s - d, yt * s - d, xt * s - d + s, yt * s - d + s)

def calculateWMBounds(bounds):
  xm0, ym0 = wgs2wm(bounds[0], bounds[1])
  xm1, ym1 = wgs2wm(bounds[2], bounds[3])
  return (xm0, ym0, xm1, ym1)

def boundsIntersect(bounds0, bounds1):
  if bounds0[0] >= bounds1[2] or bounds0[2] < bounds1[0]:
    return False
  if bounds0[1] >= bounds1[3] or bounds0[3] < bounds1[1]:
    return False
  return True

def calculateTileQuadIndex(zoom, xt, yt):
  return zoom + (((yt << zoom) + xt) << LEVEL_BITS)

def calculateGeometryQuadIndex(bounds):
  geomBounds = calculateWMBounds(bounds)
  tile = (0, 0, 0)
  while tile[0] < MAX_LEVEL:
    hitSubTiles = []
    for i in range(0, 4):
      subTile = (tile[0] + 1, tile[1] * 2 + i // 2, tile[2] * 2 + i % 2)
      subTileBounds = calculateTileBounds(*subTile)
      if boundsIntersect(subTileBounds, geomBounds):
        hitSubTiles.append(subTile)
    if len(hitSubTiles) != 1:
      break
    tile = hitSubTiles[0]
  return calculateTileQuadIndex(*tile)

def findGeometries(x, y, radius, geomQuery):
  xm, ym = wgs2wm(x, y)
  maxDist = radius / math.cos(y * math.pi / 180.0)

  results = []
  tileCount, geomCount = 0, 0
  for level in range(MAX_LEVEL, -1, -1):
    tile0 = calculatePointTile(xm - maxDist, ym - maxDist, level)
    tile1 = calculatePointTile(xm + maxDist, ym + maxDist, level)
    quadIndices = [calculateTileQuadIndex(level, xt, yt) for xt in range(tile0[1], tile1[1] + 1) for yt in range(tile0[2], tile1[2] + 1)]
    tileCount += len(quadIndices)
    for id, geom in geomQuery(quadIndices):
      geomCount += 1
      geomTransformed = shapely.ops.transform(lambda x, y: transform(inProj, outProj, x, y), geom)
      dist = shapely.geometry.Point(xm, ym).distance(geomTransformed)
      if dist <= maxDist:
        results.append((dist, id))
  results.sort(lambda result1, result2: cmp(result1[0], result2[0]))
  return results
