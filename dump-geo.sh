#!/bin/bash

set -e

# These are instructions to dump the geo database

# Prepare the environment
sudo apt update
sudo apt install -y wget unzip transmission-cli

# To begin download planet.osm.pbf using torrent
transmission-cli --download-dir ./ https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent
mv planet-*.osm.pbf planet.osm.pbf

# Download timezone data
wget "https://github.com/evansiroky/timezone-boundary-builder/releases/download/2023b/timezones-with-oceans.geojson.zip"
unzip timezones-with-oceans.geojson.zip
rm timezones-with-oceans.geojson.zip

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

# Create zip file
zip -9 planet_coarse_boundaries.zip planet_coarse_boundaries.txt
