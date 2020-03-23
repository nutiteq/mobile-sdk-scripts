# Build RTree index for spr table of Whosinfirst database to perform fast bounding box queries.

import argparse
import json
import sqlite3

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument(dest='wof', help='name of Whosonfirst database file (.db)')
  args = parser.parse_args()

  db = sqlite3.connect(args.wof)
  db.execute("BEGIN")
  db.execute("DROP TABLE IF EXISTS spr_index")
  db.execute("CREATE VIRTUAL TABLE spr_index USING rtree(id, min_latitude, max_latitude, min_longitude, max_longitude)")
  cursor1 = db.cursor()
  cursor2 = db.cursor()
  cursor1.execute("SELECT id, body FROM geojson")
  for id, body in cursor1:
    geojson = json.loads(body)
    lng0, lat0, lng1, lat1 = geojson["bbox"]
    cursor2.execute("INSERT INTO spr_index(id, min_latitude, max_latitude, min_longitude, max_longitude) VALUES(?, ?, ?, ?, ?)", (id, lat0, lat1, lng0, lng1))
  cursor2.close()
  cursor1.close()
  db.commit()

if __name__ == "__main__":
  main()
