# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

from tqdm import tqdm
from shapely.wkt import dumps
import orjson


# +
# %%time
with open('13_266069_040_003 L02 PAS.json', 'r') as file:
    data = orjson.loads(file.read())

import shapely
from shapely.geometry import shape
# -

len(data),data[0]

# ### Convert Geojson to String (orjson)

# %%time
for _ in range(12):
        for geojson in tqdm(data):
            name = geojson["properties"]["classification"]["name"]
            geometry = orjson.dumps(geojson["geometry"]).decode('ascii')

# ### Convert geojson to shapely

# %%time
for _ in range(12):
    for geojson in tqdm(data):
        name = geojson["properties"]["classification"]["name"]
        shapely_geom = shape(geojson["geometry"])

# ### Convert Geojson to Shapely to WKT

# %%time
for _ in range(12):
    for geojson in tqdm(data):
        name = geojson["properties"]["classification"]["name"]
        shapely_geom = shape(geojson["geometry"])
        wkt=shapely_geom.wkt

# ### Convert Geojson to WKB

# %%time
for _ in range(12):
    for geojson in tqdm(data):
        name = geojson["properties"]["classification"]["name"]
        shapely_geom = shape(geojson["geometry"])
        wkb=shapely_geom.wkb

# ## Convert WKB to Shapely object

# ### first generate WKB from geojsons

# %%time
wkbs=[]
for geojson in tqdm(data):
    name = geojson["properties"]["classification"]["name"]
    shapely_geom = shape(geojson["geometry"])
    wkbs.append(shapely_geom.wkb)

# ### now convert the WKB back to shapely objects

# %%time
import shapely.wkb
for _ in range(12):
    for wkb in tqdm(wkbs):
        shapely_geom = shapely.wkb.loads(wkb)

# # Check multiproc

import ray

ray.init()

ray.cluster_resources()


@ray.remote
def bulk_convert(geojsongeoms):
    wkts=[]
    for geojsongeom in geojsongeoms:
        shapely_geom = shape(geojsongeom)
        wkt=shapely_geom.wkt
        wkts.append(wkt)
    return(wkts)


# +
# %%time
futures = [] 
for _ in range(12):
    batch_size=10_000
    polygons=[]

    for geojson in tqdm(data):
        name = geojson["properties"]["classification"]["name"]
        geom = geojson["geometry"]
    
        polygons.append(geom)
    
        if len(polygons) == batch_size:
            futures.append(bulk_convert.remote(polygons))
            polygons=[]
            
    if polygons:
        futures.append(bulk_convert.remote(polygons))
    
for f in tqdm(futures):
    ray.get(f)
