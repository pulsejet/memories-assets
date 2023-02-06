import shapely
import shapely.wkt
from tqdm import tqdm
from joblib import Parallel, delayed
from shapely.validation import make_valid
from shapely.geometry import polygon

import csv
import json

# Increase the field size limit
import sys
csv.field_size_limit(sys.maxsize)

CSV_PATH = 'csv/multipolygons.csv'

def get_tolerance(admin_level):
    if admin_level == -1:
        return 0.005 # Unknown
    elif admin_level <= 4:
        return 0.01 # country or state
    elif admin_level <= 6:
        return 0.005 # City / County
    else:
        return 0.003

def process_polygon(polystr, tolerance):
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

    new_geom = list([
        list([round(c[0], 4), round(c[1], 4)] for c in poly.exterior.coords)
        for poly in geoms if poly.is_valid
    ])

    if len(new_geom) == 0:
        return None

    return new_geom

def process_row(row):
    osm_id, name, admin_level, polystr = row
    if not osm_id or not name:
        return

    # make sure admin level is an integer
    try:
        admin_level = int(admin_level)
    except:
        admin_level = -1

    # Process the polygon
    new_geom = process_polygon(polystr, get_tolerance(admin_level))
    if not new_geom:
        return

    # escape ' in name
    name = name.replace("'", "''")

    # Dump as JSON
    return json.dumps({
        'osm_id': osm_id,
        'name': name,
        'admin_level': admin_level,
        'geometry': new_geom
    })

if __name__ == '__main__':
    with open(CSV_PATH, 'r') as csvdb, open('planet_coarse_boundaries.txt', 'w') as f:
        reader = csv.DictReader(csvdb, delimiter='\t')
        itr = tqdm(reader)

        queue = []
        def flush():
            res = Parallel(n_jobs=32)(delayed(process_row)(row) for row in queue)
            [f.write(r+"\n") for r in res if r]
            queue.clear()

        for row in itr:
            row = (row['osm_id'], row['name'], row['admin_level'], row['WKT'])
            queue.append(row)
            if len(queue) > 1000:
                flush()
        flush()
