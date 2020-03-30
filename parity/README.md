# Scripts for building Parity offline packages for CARTO Mobile SDK

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

### Regenerating package tilemasks

**This is an optional step that is normally not needed, as the tilemask file is already generated!**

First step is to download border polygons for the packages:

```
curl -L https://github.com/mapsme/omim/archive/master.zip --output omim.zip
unzip omim.zip "omim-master/data/borders/*"
```

After this step it is recommended to remove Antarctica from the files. Then package template
file can be generated using:

```
python3 ../scripts/build_tilemasks.py omim-master/data/borders data/packages-parity.json.template
```

'data/packages-parity.json.template' file will be created that is needed as an input for other stages.
The whole process takes around 30 minutes.


## Creating online tiles

### Download POIs

Download POIs from CARTO account using 'download_pois' script:

```
python3 scripts/download_pois.py --quality=0.3 POI_DATA_DIRECTORY
```

The quality option defines minimum quality threshold for POIs.
POI_DATA_DIRECTORY is the directory where to keep POI data needed for next steps.
This step may take a few hours, depending on the network connection bandwidth.

### Build .mbtiles file containing the POI layer

```
python3 scripts/build_pois_mbtiles.py --quality 0.55 --filter data/parity_online_filter.txt POI_DATA_DIRECTORY POI_MBTILES_FILE
```

This step should take about 6 hours when using a 8-core CPU.

### Merge the generated POI layer to existing OMT planet file

A prerequisite for this step is the large 'planet-omt.mbtiles' file (about 70GB) that will be
merged with the generated POI .mbtiles file.

```
python3 scripts/merge_pois_mbtiles.py PLANET_OMT_MBTILES_FILE POI_MBTILES_FILE MERGED_PLANET_MBTILES_FILE
```

Here PLANET_OMT_MBTILES_FILE is the name of the original OMT planet file that will be merged with
the generated POIs. MERGED_PLANET_MBTILES_FILE file is the name of the final .mbtiles file that can be used in online service.
This step should take about 24 hours when using a 8-core CPU.


## Creating offline tile packages

### Download POIs

Basically the same step as when creating online tiles.

### Build .mbtiles file containing the POI layer

The step is almost the same as when creating online tiles but with different 'filter' option value:

```
python3 scripts/build_pois_mbtiles.py --quality 0.55 --filter data/parity_offine_filter.txt POI_DATA_DIRECTORY POI_MBTILES_FILE
```

### Merge HERE POI layer with OpenMapTiles layers

Basically the same step as when creating online tiles.

### Build offline map packages

```
python3 ../scripts/build_map_packages.py data/packages-parity.json.template MERGED_PLANET_MBTILES_FILE PACKAGES_DIRECTORY
```

The individual package .mbtiles files are placed into PACKAGES_DIRECTORY. Also, the script
generates 'packages.json' file that has URLs to the individual packages. Once the packages are
uploaded, the URLs in this file need to be updated and the 'packages.json' can the be uploaded.
The step should take about 20 hours using a 8-core CPUs.


## Creating Valhalla packages

This step is the same as creating Valhalla packages for CARTO sources as POI data is not used for routing.


## Creating offline geocoding packages

### Download POIs

Basically the same step as when creating online tiles.

### Prepare WhosOnFirst gazetter

First download latest WhosOnFirst gazetter database as Sqlite file from [https://dist.whosonfirst.org/sqlite/]

Create R-Tree index for fast spatial queries using the downloaded database:

```
python3 ../scripts/build_wof_index.py WHOSONFIRST_FILE
```

This should take 5-10mins.

### Prepare package-based OpenStreetMap files

First download the large planet.osm.pbf (50GB) file. Build the package extracts:

```
python3 ../scripts/extract_package_pbfs.py data/packages-parity.json.template PLANET_OSM_PBF_FILE PBF_EXTRACT_DIRECTORY
```

This step should take around 24 hours using a 8-core CPU. The amount of memory is critical, at least 32GB is required.

### Extract addresses from OpenStreetMap files

This steps extracts addresses, POIs, streets and buildings from .pbf extracts:

```
python3 ../scripts/build_osm_addresses.py data/packages-parity.json.template PBF_EXTRACT_DIRECTORY OSM_ADDRESS_DIRECTORY
```

This should take around 20 hours using a 8-core CPU.

### Merge HERE POI data with OpenStreetMap data

This step replaces OpenStreetMap POI data with HERE POI data:

```
python3 scripts/merge_pois_addresses.py --quality 0.55 --filter data/parity_offine_filter.txt data/packages-parity.json.template OSM_ADDRESS_DIRECTORY POI_DATA_DIRECTORY MERGED_ADDRESS_DIRECTORY
```

This should take around 8 hours using a 8-core CPU.

### Build offline geocoding packages

```
python3 ../scripts/build_geocoding_packages.py data/packages-parity.json.template MERGED_ADDRESS_DIRECTORY WHOSONFIRST_FILE PACKAGES_DIRECTORY
```

The individual package .nutigeodb files are placed into PACKAGES_DIRECTORY. Also, the script
generates 'packages.json' file that has URLs to the individual packages. Once the packages are
uploaded, the URLs in this file need to be updated and the 'packages.json' can the be uploaded.
The step should take about 12 hours using a 8-core CPU.
