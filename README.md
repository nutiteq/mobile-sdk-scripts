# Scripts for building offline packages for CARTO Mobile SDK

### General prerequisites

* At least 32GB of RAM
* At least 200GB of free disk space
* python 3.6+
* *pip* for python3
* *protobuf* python package (`pip install protobuf`)
* *pyproj* python package (`pip install pyproj`)
* *shapely* python package (`pip install shapely`)
* *osmium* python package (`pip install osmium`)

It is highly recommended to use *PyPy* instead of *CPython*, as it can speed up processing several times.
Also, it is highly recommended to use solid state storage for the input files and output files as
the workloads are very IO intensive.

### Regenerating package list and tilemasks

**This is an optional step that is normally not needed!** This is needed only if a different structure of offline packages is needed.

A prerequisite for this is a directory containing .poly files

```
python3 scripts/build_tilemasks.py POLY_FILE_DIRECTORY PACKAGES_TEMPLATE_FILE
```

PACKAGES_TEMPLATE_FILE file will be created that is needed as an input for other stages.
The whole process takes around 30 minutes, depending on the complexity of .poly files.


## Creating offline map packages

### Build map packages

A prerequisite for this is a 'planet.mbtiles' file containing tiles for the whole planet (about 70GB).

```
python3 scripts/build_map_packages.py data/packages-carto.json.template PLANET_MBTILES_FILE PACKAGES_DIRECTORY
```

The individual package .mbtiles files are placed into PACKAGES_DIRECTORY. Also, the script
generates 'packages.json' file that has URLs to the individual packages. Once the packages are
uploaded, the URLs in this file need to be updated and the 'packages.json' can the be uploaded.
The step should take about 20 hours using a 8-core CPU.

### Build and use shared dictionaries for the generated packages

This step is optional and reduces the total size of the packages by about 5-10%, though some packages like Greenland can be up to 2x smaller after this.

```
python scripts/zdict_from_packages "PACKAGES_DIRECTORY/*.mbtiles" ZDICT_DIRECTORY
```

This step should take around 4 hours to complete. As a result .zdict files are created and stored in ZDICT_DIRECTORY.

After dictonary files are created, the packages can be recreated using .zdict files:

```
python3 scripts/build_map_packages.py --zdict ZDICT_DIRECTORY data/packages-carto.json.template PLANET_MBTILES_FILE PACKAGES_DIRECTORY
```

The generated .mbtiles files after this step are no longer usable with other SDKs or tools, as this utilizes custom .mbtiles extension only supported by CARTO Mobile SDK.


## Creating Valhalla routing packages

A prerequisite for this is Valhalla 3 installation.

### Build Valhalla tiles

First download the large 'planet.osm.pbf' (50GB) file. Build the package extracts:

```
valhalla_build_tiles -c data/valhalla.json PATH_TO_OSM_PBF_FILE
```

This step should take abound 24 hours.

### Build offline Valhalla packages

```
python3 scripts/build_valhalla_packges.py data/packages-carto.json.template valhalla_tiles PACKAGES_DIRECTORY
```

The individual package .vtiles files are placed into PACKAGES_DIRECTORY. Also, the script
generates 'packages.json' file that has URLs to the individual packages. Once the packages are
uploaded, the URLs in this file need to be updated and the 'packages.json' can the be uploaded.
The step should take about 12 hours using a 8-core CPU.


## Creating offline geocoding packages

### Prepare WhosOnFirst gazetter

First download latest WhosOnFirst gazetter database as Sqlite file from [https://dist.whosonfirst.org/sqlite/]

Create R-Tree index for fast spatial queries using the downloaded database:

```
python3 scripts/build_wof_index.py WHOSONFIRST_FILE
```

This should take 5-10mins.

### Prepare package-based OpenStreetMap files

First download the large 'planet.osm.pbf' (50GB) file. Build the package extracts:

```
python3 scripts/extract_package_pbfs.py data/packages-carto.json.template PLANET_OSM_PBF_FILE PBF_EXTRACT_DIRECTORY
```

This step should take around 24 hours using a 8-core CPU. The amount of memory is critical, at least 32GB is required.

### Extract addresses from OpenStreetMap files

This steps extracts addresses, POIs, streets and buildings from .pbf extracts:

```
python3 scripts/build_osm_addresses.py data/packages-carto.json.template PBF_EXTRACT_DIRECTORY OSM_ADDRESS_DIRECTORY
```

This should take around 20 hours using a 8-core CPU.

### Build offline geocoding packages

```
python3 scripts/build_geocoding_packages.py data/packages-carto.json.template OSM_ADDRESS_DIRECTORY WHOSONFIRST_FILE PACKAGES_DIRECTORY
```

The individual package .nutigeodb files are placed into PACKAGES_DIRECTORY. Also, the script
generates 'packages.json' file that has URLs to the individual packages. Once the packages are
uploaded, the URLs in this file need to be updated and the 'packages.json' can the be uploaded.
The step should take about 12 hours using a 8-core CPU.
