import shapely.geometry
import shapely.ops

def validateGeometry(geometry):
  if geometry['type'].lower() == 'geometrycollection':
    return all([validateGeometry(subGeom) for subGeom in geometry['geometries']])
  if geometry['type'][:5].lower() == 'multi':
    return all([validateGeometry({ 'type': geometry['type'][5:], 'coordinates': coords }) for coords in geometry['coordinates']])
  try:
    return shapely.geometry.shape(geometry).is_valid
  except:
    return False

def simplifyGeometry(geometry, factor):
  if geometry['type'].lower() in ('point', 'multipoint'):
    return geometry
  if geometry['type'].lower() == 'geometrycollection':
    subGeoms = []
    for subGeom in geometry['geometries']:
      subGeom = simplifyGeometry(subGeom, factor)
      if subGeom is not None:
        subGeoms.append(subGeom)
    geometry = dict(geometry)
    geometry['geometries'] = subGeoms
    return geometry
  if geometry['type'].lower() == 'multipolygon':
    return { 'type': geometry['type'], 'coordinates': [simplifyGeometry({ 'type': 'Polygon', 'coordinates': coords }, factor)['coordinates'] for coords in geometry['coordinates']] }
  geom = shapely.geometry.shape(geometry)
  if geometry['type'].lower() == 'multilinestring':
    if len(geom.geoms) == 0:
      return None
    try:
      geom = shapely.ops.linemerge(geom.geoms)
    except:
      print('GeomUtils: Failed to perform linemerge')
  if getattr(geom, 'geoms', None) is not None:
    if len(geom.geoms) == 0:
      print('GeomUtils: No geometries no multigeometry')
      return None
  size = max(geom.bounds[2] - geom.bounds[0], geom.bounds[3] - geom.bounds[1])
  try:
    geom = geom.simplify(size * factor)
    return shapely.geometry.mapping(geom)
  except:
    print('GeomUtils: Failed to perform simplify')
    return geometry

def mergeGeometries(geometries):
  def listGeometries(geometry):
    geoms = []
    if geometry['type'].lower() == 'geometrycollection':
      for subGeom in geometry['geometries']:
        geoms += listGeometries(subGeom)
    else:
      geoms = [geometry]
    return geoms

  geomList = []
  for geometry in geometries:
    geomList += [shapely.geometry.shape(subGeom) for subGeom in listGeometries(geometry)]
  try:
    if any([isinstance(geom, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon)) for geom in geomList]):
      geomList = [geom for geom in geomList if isinstance(geom, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))]
    elif any([isinstance(geom, (shapely.geometry.LineString, shapely.geometry.MultiLineString)) for geom in geomList]):
      geomList = [geom for geom in geomList if isinstance(geom, (shapely.geometry.LineString, shapely.geometry.MultiLineString))]
    elif any([isinstance(geom, (shapely.geometry.Point, shapely.geometry.MultiPoint)) for geom in geomList]):
      geomList = [geom for geom in geomList if isinstance(geom, (shapely.geometry.Point, shapely.geometry.MultiPoint))]
    geom = shapely.ops.unary_union(geomList)
    return shapely.geometry.mapping(geom)
  except:
    print('GeomUtils: Failed to unify geometry list')
    return { 'type': 'GeometryCollection', 'geometries': listGeometries(geometry) }

def calculateGeometryBounds(geometry):
  if geometry['type'].lower() == 'geometrycollection':
    boundsList = [calculateGeometryBounds(subGeom) for subGeom in geometry['geometries']]
    xs0 = [bounds[0] for bounds in boundsList]
    ys0 = [bounds[1] for bounds in boundsList]
    xs1 = [bounds[2] for bounds in boundsList]
    ys1 = [bounds[3] for bounds in boundsList]
    return (min(xs0), min(ys0), max(xs1), max(ys1)) if boundsList else None
  points = []
  if geometry['type'].lower() == 'multipolygon':
    points = [point for rings in geometry['coordinates'] for ring in rings for point in ring]
  elif geometry['type'].lower() in ('polygon', 'multilinestring'):
    points = [point for ring in geometry['coordinates'] for point in ring]
  elif geometry['type'].lower() in ('linestring', 'multipoint'):
    points = [point for point in geometry['coordinates']]
  elif geometry['type'].lower() == 'point':
    points = [geometry['coordinates']]
  xs = [float(point[0]) for point in points]
  ys = [float(point[1]) for point in points]
  return (min(xs), min(ys), max(xs), max(ys)) if points else None

def testClipBounds(bounds, clipBounds):
  if bounds is None or clipBounds is None:
    return False
  if bounds[0] > clipBounds[2] or bounds[2] < clipBounds[0] or bounds[1] > clipBounds[3] or bounds[3] < clipBounds[1]:
    return False
  return True

def mergeBounds(bounds1, bounds2):
  if bounds1 is None:
    return bounds2
  if bounds2 is None:
    return bounds1
  return (min(bounds1[0], bounds2[0]), min(bounds1[1], bounds2[1]), max(bounds1[2], bounds2[2]), max(bounds1[3], bounds2[3]))

def mergeBoundsLists(boundsList1, boundsList2):
  boundsList = boundsList1
  for bounds in boundsList2:
    skip = False
    for i in range(0, len(boundsList)):
      bounds2 = boundsList[i]
      if bounds[0] >= bounds2[0] and bounds[1] >= bounds2[1] and bounds[2] <= bounds2[2] and bounds[3] <= bounds2[3]:
        skip = True
        break
      if bounds[0] <= bounds2[0] and bounds[1] <= bounds2[1] and bounds[2] >= bounds2[2] and bounds[3] >= bounds2[3]:
        boundsList[i] = bounds
        skip = True
        break
    if not skip:
      boundsList.append(bounds)
  return boundsList
