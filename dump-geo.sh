#!/bin/bash

# These are instructions to dump the geo database
# To begin download planet.osm.pbf using wget / torrent

# Install GDAL and OSMIUM
sudo apt install -y gdal-bin osmium-tool python3 python3-pip

# Install geo python deps
python3 -m pip install shapely joblib tqdm

# Extract boundaries from planet
osmium tags-filter planet.osm.pbf wra/boundary=administrative -o boundaries.osm.pbf

# Convert boundaries to CSV format
mkdir -p csv && cd csv
ogr2ogr -f "CSV" -lco SEPARATOR="TAB" -lco GEOMETRY=AS_WKT -overwrite -makevalid boundaries.csv ../boundaries.osm.pbf
cd ..

# This will create multipolygons.csv that contains the data we need. To create the SQL file:
python3 dump-geo.py
