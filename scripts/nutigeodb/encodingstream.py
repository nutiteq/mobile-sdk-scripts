import struct
import shapely

PRECISION = 1.0e6

class EncodingStream(object):
  def __init__(self, data=None):
    self.offset = 0
    self.data = bytearray(data) if data else bytearray()

  def eof(self):
    return self.offset >= len(self.data)

  def getData(self):
    return bytes(self.data)

  def encodeNumber(self, num):
    if num < 0:
      num = (-num << 1) - 1
    else:
      num = num << 1
    shift = 7
    while (num >> shift) > 0:
      shift += 7
    while shift > 0:
      shift -= 7
      val = (num >> shift) & 127
      self.data.append(val + (128 if shift > 0 else 0))

  def decodeNumber(self):
    num = 0
    while True:
      val = int(self.data[self.offset])
      self.offset += 1
      num = num + (val & 127)
      if val < 128:
        break
      num = num << 7
    if (num & 1) == 1:
      num = -((num + 1) >> 1)
    else:
      num = num >> 1
    return num

  def encodeString(self, str):
    bstr = str.encode('utf8')
    self.encodeNumber(len(bstr))
    self.data.extend(bstr)

  def decodeString(self):
    n = self.decodeNumber()
    bstr = self.data[self.offset:self.offset+n]
    self.offset += n
    return str(bstr, encoding='utf8')

  def encodeValue(self, value):
    if value is None:
      self.encodeNumber(0)
    elif isinstance(value, bool):
      self.encodeNumber(1)
      self.encodeNumber(1 if value else 0)
    elif isinstance(value, int):
      self.encodeNumber(2)
      self.encodeNumber(value)
    elif isinstance(value, float):
      self.encodeNumber(3)
      bstr = struct.pack('!f', value)
      self.data.extend(bstr)
    elif isinstance(value, str) or isinstance(value, unicode):
      self.encodeNumber(4)
      self.encodeString(value)
    else:
      raise ValueError("Unsupported value type for encoding")

  def decodeValue(self):
    type = self.decodeNumber()
    if type == 0:
      return None
    elif type == 1:
      return self.decodeNumber() != 0
    elif type == 2:
      return self.decodeNumber()
    elif type == 3:
      bstr = self.data[self.offset:self.offset+4]
      self.offset += 4
      return struct.unpack('!f', bstr)
    elif type == 4:
      return self.decodeString()

class DeltaEncodingStream(EncodingStream):
  def __init__(self, data=None, prevCoord=(0, 0), prevNumber=0):
    super(DeltaEncodingStream, self).__init__(data)
    x = int(round(prevCoord[0] * PRECISION))
    y = int(round(prevCoord[1] * PRECISION))
    self.prevCoord = [x, y]
    self.prevNumber = prevNumber

  def deltaEncodeNumber(self, num):
    super(DeltaEncodingStream, self).encodeNumber(num - self.prevNumber)
    self.prevNumber = num

  def deltaDecodeNumber(self):
    delta = super(DeltaEncodingStream, self).decodeNumber()
    num = self.prevNumber + delta
    self.prevNumber = num
    return num

  def encodeCoord(self, coord):
    x = int(round(coord[0] * PRECISION))
    y = int(round(coord[1] * PRECISION))
    dx = x - self.prevCoord[0]
    dy = y - self.prevCoord[1]
    self.encodeNumber(dx)
    self.encodeNumber(dy)
    self.prevCoord[0] = x
    self.prevCoord[1] = y
  
  def decodeCoord(self):
    dx = self.decodeNumber()
    dy = self.decodeNumber()
    x = self.prevCoord[0] + dx
    y = self.prevCoord[1] + dy
    coord = (x / PRECISION, y / PRECISION)
    self.prevCoord[0] = x
    self.prevCoord[1] = y
    return coord

  def encodeCoords(self, coords):
    self.encodeNumber(len(coords))
    for coord in coords:
      self.encodeCoord(coord)

  def decodeCoords(self):
    n = self.decodeNumber()
    coords = []
    for i in range(0, n):
      coord = self.decodeCoord()
      coords.append(coord)
    return coords

  def encodeRings(self, rings):
    encodeNumber(len(rings))
    for ring in rings:
      self.encodeCoords(ring)

  def decodeRings(self):
    n = self.decodeNumber()
    rings = []
    for i in range(0, n):
      ring = self.decodeCoords()
      rings.append(ring)
    return rings

  def encodeGeometry(self, geom):
    if geom is None:
      self.encodeNumber(0)
    elif geom['type'] == 'Point':
      self.encodeNumber(1)
      self.encodeCoord(geom['coordinates'])
    elif geom['type'] == 'MultiPoint':
      self.encodeNumber(2)
      self.encodeCoords(geom['coordinates'])
    elif geom['type'] == 'LineString':
      self.encodeNumber(3)
      self.encodeCoords(geom['coordinates'])
    elif geom['type'] == 'MultiLineString':
      self.encodeNumber(4)
      self.encodeNumber(len(geom['coordinates']))
      for coords in geom['coordinates']:
        self.encodeCoords(coords)
    elif geom['type'] == 'Polygon':
      self.encodeNumber(5)
      self.encodeNumber(len(geom['coordinates']))
      for ring in geom['coordinates']:
        self.encodeCoords(ring[:-1] if ring[0] == ring[-1] else ring)
    elif geom['type'] == 'MultiPolygon':
      self.encodeNumber(6)
      self.encodeNumber(len(geom['coordinates']))
      for rings in geom['coordinates']:
        self.encodeNumber(len(rings))
        for ring in rings:
          self.encodeCoords(ring[:-1] if ring[0] == ring[-1] else ring)
    elif geom['type'] == 'GeometryCollection':
      self.encodeNumber(7)
      self.encodeNumber(len(geom['geometries']))
      for subgeom in geom['geometries']:
        self.encodeGeometry(subgeom)
    else:
      raise ValueError('Invalid geometry type')

  def decodeGeometry(self):
    type = self.decodeNumber()
    if type == 0:
      return None
    elif type == 1:
      coord = self.decodeCoord()
      return { 'type': 'Point', 'coordinates': coord }
    elif type == 2:
      coords = self.decodeCoords()
      return { 'type': 'MultiPoint', 'coordinates': coords }
    elif type == 3:
      coords = self.decodeCoords()
      return { 'type': 'LineString', 'coordinates': coords }
    elif type == 4:
      n = self.decodeNumber()
      coordsList = []
      for i in range(0, n):
        coords = self.decodeCoords()
        coordsList.append(coords)
      return { 'type': 'MultiLineString', 'coordinates': coordsList }
    elif type == 5:
      n = self.decodeNumber()
      rings = []
      for i in range(0, n):
        ring = self.decodeCoords()
        rings.append(ring + [ring[0]])
      return { 'type': 'Polygon', 'coordinates': rings }
    elif type == 6:
      n = self.decodeNumber()
      ringsList = []
      for i in range(0, n):
        n = self.decodeNumber()
        rings = []
        for i in range(0, n):
          ring = self.decodeCoords()
          rings.append(ring + [ring[0]])
          ringsList.append(rings)
      return { 'type': 'MultiPolygon', 'coordinates': ringsList }
    elif type == 7:
      n = self.decodeNumber()
      subgeoms = []
      for i in range(0, n):
        subgeoms.append(self.decodeGeometry())
      return { 'type': 'GeometryCollection', 'geometries': subgeoms }
    else:
      raise ValueError('Unexpected type code')

  def encodeFeature(self, feature):
    self.deltaEncodeNumber(feature.get('id', 0))
    self.encodeGeometry(feature.get('geometry', None))
    self.encodeNumber(len(feature.get('properties', {})))
    for name, value in feature.get('properties', {}).items():
      self.encodeString(name)
      self.encodeValue(value)

  def decodeFeature(self):
    id = self.deltaDecodeNumber()
    geometry = self.decodeGeometry()
    n = self.decodeNumber()
    properties = {}
    for i in range(0, n):
      name = self.decodeString()
      value = self.decodeValue()
      properties[name] = value
    return { 'id': id, 'geometry': geometry, 'properties': properties }

  def encodeFeatureCollection(self, featureCollection):
    self.encodeNumber(len(featureCollection))
    for feature in featureCollection:
      self.encodeFeature(feature)

  def decodeFeatureCollection(self):
    n = self.decodeNumber()
    features = []
    for i in range(0, n):
      features.append(self.decodeFeature())
    return features
