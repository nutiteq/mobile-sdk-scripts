import json
import shapely
import shapely.prepared
import shapely.geometry
import sqlite3

class WOFLocator(object):
  def __init__(self, db, placetypes):
    self.db = db
    self.cursor = self.db.cursor()
    self.placetypes = placetypes
    self.geoJSONCache = {}
    self.hierarchyCache = {}

  def findGeoJSON(self, id):
    if id not in self.geoJSONCache:
      self.cursor.execute("SELECT body FROM geojson WHERE id=?", (id,))
      row = self.cursor.fetchone()
      geojson = json.loads(row[0]) if row else { 'type': 'Feature', 'geometry': { 'type': 'GeometryCollection', 'geometries': [] } }
      self.geoJSONCache[id] = geojson
    return self.geoJSONCache.get(id, None)

  def getGeometryAndHierarchy(self, id):
    if id not in self.hierarchyCache:
      self.cursor.execute("SELECT body FROM geojson WHERE id=?", (id,))
      body = self.cursor.fetchone()[0]
      geojson = json.loads(body)
      geometry = shapely.prepared.prep(shapely.geometry.asShape(geojson['geometry']))
      hierarchy = []
      for places in geojson['properties'].get('wof:hierarchy', []):
        hierarchy.append({ key: val for key, val in places.items() if key.endswith('_id') and key[:-3] in self.placetypes })
      self.hierarchyCache[id] = (geometry, hierarchy)
    return self.hierarchyCache[id]

  def getHierarchy(self, id):
    return self.getGeometryAndHierarchy(id)[1]

  def findHierarchy(self, pos):
    parents = []
    self.cursor.execute("SELECT s.id, s.placetype, s.is_current FROM spr_index si, spr s WHERE si.min_longitude<=? AND si.min_latitude<=? AND si.max_longitude>=? AND si.max_latitude>=? AND si.id=s.id ORDER BY si.max_longitude-si.min_longitude DESC", (pos[0], pos[1], pos[0], pos[1]))
    for id, placetype, current in self.cursor:
      if current and placetype in self.placetypes:
        parents.append((id, placetype))
    parents.sort(key=lambda parent: -self.placetypes.index(parent[1]))
    for id, placetype in parents:
      geometry, hierarchy = self.getGeometryAndHierarchy(id)
      if geometry.contains(shapely.geometry.Point(*pos)):
        return hierarchy
    return []
