import json
import pickle
import shapely
import shapely.prepared
import shapely.geometry
import sqlite3
from nutigeodb.geomutils import *

class BuildingsLocator(object):
  def __init__(self, db):
    self.db = db
    self.cursor = self.db.cursor()

  def initialize(self):
    self.db.execute("CREATE TABLE IF NOT EXISTS buildings_data(id INTEGER PRIMARY KEY, geometry TEXT NOT NULL, data TEXT NOT NULL)")
    self.db.execute("CREATE VIRTUAL TABLE buildings_index USING rtree(id, min_latitude, max_latitude, min_longitude, max_longitude)")

  def finish(self):
    self.db.execute("DROP TABLE IF EXISTS buildings_index")
    self.db.execute("DROP TABLE IF EXISTS buildings_data")

  def findGeometry(self, geometry):
    results = []
    bounds = calculateGeometryBounds(geometry)
    self.cursor.execute("SELECT bd.geometry, bd.data FROM buildings_data bd, buildings_index bi WHERE bi.min_longitude<=? AND bi.min_latitude<=? AND bi.max_longitude>=? AND bi.max_latitude>=? AND bi.id=bd.id", (bounds[0], bounds[1], bounds[2], bounds[3]))
    for row in self.cursor:
      try:
        buildingShape = shapely.geometry.asShape(pickle.loads(row[0]))
        if shapely.geometry.asShape(geometry).within(buildingShape):
          data = pickle.loads(row[1])
          results.append(data)
      except:
        pass
    return results

  def importGeometry(self, geometry, data):
    bounds = calculateGeometryBounds(geometry)
    encodedGeometry = pickle.dumps(geometry)
    encodedData = pickle.dumps(data)
    self.cursor.execute("INSERT INTO buildings_data(geometry, data) VALUES(?, ?)", (encodedGeometry, encodedData))
    self.cursor.execute("INSERT INTO buildings_index(id, min_latitude, max_latitude, min_longitude, max_longitude) VALUES(?, ?, ?, ?, ?)", (self.cursor.lastrowid, bounds[1], bounds[3], bounds[0], bounds[2]))
