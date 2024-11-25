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


# Connect to the Spatialite database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# In[9]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute("PRAGMA timeout= 1000;")    


# In[10]:


# Create the table
Base.metadata.create_all(engine)


# In[11]:


from tqdm import tqdm


# In[16]:


@ray.remote
def bulk_insert(polygons):
    engine = create_engine('sqlite:////tmp/blog/spatialite_ray.db')#,echo=True)
    
    try:
        # Initialize Spatialite extension
        @event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            dbapi_connection.enable_load_extension(True)
            dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
            dbapi_connection.execute("PRAGMA busy_timeout= 10000000;")   
    
        with engine.connect() as conn:
            with conn.begin() as tran:
                res=conn.execute(text("INSERT INTO geometries (name,geom) VALUES (:name,GeomFromGeoJSON(:geom));"),polygons)
    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# In[ ]:





# In[17]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(12):\n    batch_size=5_000\n    polygons=[]\n    \n    for geojson in tqdm(data):\n        name = geojson["properties"]["classification"]["name"]\n        geometry = orjson.dumps(geojson["geometry"],).decode("utf-8")\n    \n        polygons.append({\'name\':name,\'geom\':geometry})\n    \n        \n        if len(polygons) == batch_size:\n            futures.append(bulk_insert.remote(polygons))\n            polygons=[]\n    \n    if polygons:\n        futures.append(bulk_insert.remote(polygons))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[18]:


get_ipython().run_cell_magic('time', '', '#lets make sure insert worked as expected\nwith  engine.connect() as conn:\n    res=conn.execute(text("select count(geom) from geometries"))\n    nresults=res.fetchall()\n    print(nresults)\n')


# In[19]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    res=conn.execute(text("select AsGeoJSON(ST_centroid(geom)) from geometries limit 1000"))\n    centroids=res.fetchall()\n')


# In[20]:


centroids[0:100]


# In[ ]:




