#!/usr/bin/env python
# coding: utf-8

# In[1]:


from tqdm import tqdm
from shapely.wkt import dumps
import orjson


# In[2]:


get_ipython().run_cell_magic('time', '', "with open('13_266069_040_003 L02 PAS.json', 'r') as file:\n    data = orjson.loads(file.read())\n\nimport shapely\nfrom shapely.geometry import shape\n")


# In[3]:


len(data),data[0]


# ### Convert Geojson to String (orjson)

# In[4]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n        for geojson in tqdm(data):\n            name = geojson["properties"]["classification"]["name"]\n            geometry = orjson.dumps(geojson["geometry"]).decode(\'ascii\')\n')


# ### Convert geojson to shapely

# In[5]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        shapely_geom = shape(geojson["geometry"])\n')


# ### Convert Geojson to Shapely to WKT

# In[6]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        shapely_geom = shape(geojson["geometry"])\n        wkt=shapely_geom.wkt\n')


# ### Convert Geojson to WKB

# In[7]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        shapely_geom = shape(geojson["geometry"])\n        wkb=shapely_geom.wkb\n')


# ## Convert WKB to Shapely object

# ### first generate WKB from geojsons

# In[8]:


get_ipython().run_cell_magic('time', '', 'wkbs=[]\nfor geojson in tqdm(data):\n    name = geojson["properties"]["classification"]["name"]\n    shapely_geom = shape(geojson["geometry"])\n    wkbs.append(shapely_geom.wkb)\n')


# ### now convert the WKB back to shapely objects

# In[9]:


get_ipython().run_cell_magic('time', '', 'import shapely.wkb\nfor _ in range(12):\n    for wkb in tqdm(wkbs):\n        shapely_geom = shapely.wkb.loads(wkb)\n')


# # Check multiproc

# In[10]:


import ray


# In[11]:


ray.init()


# In[12]:


ray.cluster_resources()


# In[13]:


@ray.remote
def bulk_convert(geojsongeoms):
    wkts=[]
    for geojsongeom in geojsongeoms:
        shapely_geom = shape(geojsongeom)
        wkt=shapely_geom.wkt
        wkts.append(wkt)
    return(wkts)


# In[14]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(12):\n    batch_size=10_000\n    polygons=[]\n\n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        geom = geojson["geometry"]\n    \n        polygons.append(geom)\n    \n        if len(polygons) == batch_size:\n            futures.append(bulk_convert.remote(polygons))\n            polygons=[]\n            \n    if polygons:\n        futures.append(bulk_convert.remote(polygons))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')

