#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sqlalchemy import create_engine, Column, String, Integer, func, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry 
from tqdm import tqdm
from shapely.wkt import dumps

import orjson


# In[2]:


import ray


# In[3]:


ray.init()


# In[4]:


ray.cluster_resources()


# In[5]:


get_ipython().run_cell_magic('time', '', "with open('13_266069_040_003 L02 PAS.json', 'r') as file:\n#with open('/mnt/c/research/kidney/15_26609_024_045 L03 PAS.json', 'r') as file:\n    # Load the JSON data into a Python dictionary\n    data = orjson.loads(file.read())\n\nimport shapely\nfrom shapely.geometry import shape\n")


# In[6]:


data[0]


# In[7]:


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# In[8]:


from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test1')#,echo=True)

print(engine.url)
try:
    create_database(engine.url)
    print("created")
except:
    print("errored")
    pass


# In[9]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# In[10]:


# Create the table
Base.metadata.create_all(engine)


# In[11]:


from tqdm import tqdm


# In[12]:


@ray.remote
def bulk_insert(polygons):
    engine = create_engine('postgresql://postgres@localhost:5333/test1')#,echo=True)

    # Initialize Spatialite extension-
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
    
    try:

        with engine.connect() as conn:
            with conn.begin() as tran:
                res=conn.execute(text("INSERT INTO geometries (name,geom) VALUES (:name,ST_GeomFromGeoJSON(:geom));"),polygons)
    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(12):\n    batch_size=5_000\n    polygons=[]\n    \n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        geometry = orjson.dumps(geojson["geometry"],).decode("utf-8")\n    \n        polygons.append({\'name\':name,\'geom\':geometry})\n    \n        \n        if len(polygons) == batch_size:\n            futures.append(bulk_insert.remote(polygons))\n            polygons=[]\n    \n    if polygons:\n        futures.append(bulk_insert.remote(polygons))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '#lets make sure insert worked as expected\nwith  engine.connect() as conn:\n    res=conn.execute(text("select count(geom) from geometries"))\n    nresults=res.fetchall()\n    print(nresults)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    res=conn.execute(text("select ST_AsGeoJSON(ST_Centroid(geom)) from geometries limit 1000"))\n    centroids=res.fetchall()\n')


# In[ ]:


centroids[0:100]


# In[ ]:




