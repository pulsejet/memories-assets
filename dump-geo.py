import shapely
import shapely.wkt
import json

from tqdm import tqdm
from joblib import Parallel, delayed
from shapely.validation import make_valid
from shapely.geometry import polygon, shape

import csv
import json

# Increase the field size limit
import sys
csv.field_size_limit(sys.maxsize)

CSV_PATH = 'csv/multipolygons.csv'
TZ_GEOJSON_PATH = 'combined-with-oceans.json'

def get_tolerance(admin_level):
    if admin_level == -1:
        return 0.005 # Unknown
    elif admin_level == -7:
        return 0.0075 # Timezone
    elif admin_level <= 4:
        return 0.01 # country or state
    elif admin_level <= 6:
        return 0.005 # City / County
    else:
        return 0.003

def process_polygon(polystr, tolerance, osm_id):
    # Read the multipolygon
    mp = shapely.wkt.loads(polystr)
    if not mp:
        return None

    # Make sure the multipolygon is valid
    if mp.geom_type == 'MultiPolygon':
        pass
    elif mp.geom_type == 'Polygon':
        mp = shapely.geometry.MultiPolygon([mp])
    else:
        return None

    # Simplify the multipolygon
    polygons = []
    for poly in mp.geoms:
        simple_poly = poly.simplify(tolerance)
        valid_poly = make_valid(simple_poly)

        if valid_poly.geom_type == 'MultiPolygon':
            for i, p in enumerate(valid_poly.geoms):
                polygons.append(p)
        elif valid_poly.geom_type == 'GeometryCollection':
            for i, p in enumerate(valid_poly.geoms):
                if p.geom_type == 'Polygon':
                    polygons.append(p)
        elif valid_poly.geom_type == 'Polygon':
            polygons.append(valid_poly)

    if len(polygons) == 0:
        return None

    # Create a new multipolygon
    new_mp = make_valid(shapely.geometry.MultiPolygon(polygons))

    # Make sure the output is a multipolygon
    geoms = []
    if new_mp.geom_type == 'MultiPolygon':
        geoms = new_mp.geoms
    elif new_mp.geom_type == 'Polygon':
        geoms = [new_mp]
    elif new_mp.geom_type == 'GeometryCollection':
        geoms = [p for p in new_mp.geoms if p.geom_type == 'Polygon']
    else:
        return None

    def coords(o):
        return list([round(c[0], 4), round(c[1], 4)] for c in o)

    new_geom = list()
    for i, poly in enumerate(geoms):
        if poly.is_valid:
            id = "%s_%s" % (osm_id, i)
            # Add the exterior ring
            new_geom.append({
                "i": id,
                "k": id,
                "t": 1,
                "c": coords(poly.exterior.coords),
            })

            # Add the interior rings
            for j, interior in enumerate(poly.interiors):
                new_geom.append({
                    "i": id,
                    "k": "%s_i%s" % (id, j),
                    "t": -1,
                    "c": coords(interior.coords),
                })

    if len(new_geom) == 0:
        return None

    return new_geom

def process_row(row):
    osm_id, name, admin_level, polystr, other_tags = row
    if not osm_id or not name:
        return

    # make sure admin level is an integer
    try:
        admin_level = int(admin_level)
    except:
        admin_level = -1

    # Process the polygon
    new_geom = process_polygon(polystr, get_tolerance(admin_level), osm_id)
    if not new_geom:
        return

    # escape ' in name
    name = name.replace("'", "''")

    # add names in other languages
    other_names = {}
    try:
        if other_tags:
            other_tags = other_tags.replace('=>', ':')
            other_tags = json.loads('{' + other_tags + '}')
            for k, v in other_tags.items():
                if k.startswith('name:'):
                    other_names[k[5:]] = v
    except:
        pass

    # Dump as JSON
    return json.dumps({
        'osm_id': osm_id,
        'name': name,
        'admin_level': admin_level,
        'geometry': new_geom,
        'other_names': other_names,
    })

if __name__ == '__main__':
    with open(CSV_PATH, 'r') as csvdb, \
         open(TZ_GEOJSON_PATH, 'r') as tz_geojson, \
         open('planet_coarse_boundaries.txt', 'w') as f:

        queue = []
        def flush():
            res = Parallel(n_jobs=32)(delayed(process_row)(row) for row in queue)
            [f.write(r+"\n") for r in res if r]
            queue.clear()

        # Dump Timezone GeoJSON
        print("Processing timezone GeoJSON")
        features = json.load(tz_geojson)["features"]
        for i, feature in tqdm(enumerate(features)):
            tzid = feature['properties']['tzid']
            shp = shape(feature["geometry"])
            osm_id = -20000 + i
            queue.append((osm_id, tzid, -7, shp.wkt, ''))
        flush()

        # Dump world boundaries
        print("Processing planet boundaries")
        for row in tqdm(csv.DictReader(csvdb, delimiter='\t')):
            queue.append((row['osm_id'], row['name'], row['admin_level'], row['WKT'], row['other_tags']))
            if len(queue) > 1000:
                flush()
        flush()
