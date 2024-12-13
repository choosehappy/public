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


get_ipython().run_cell_magic('time', '', "with open('13_266069_040_003 L02 PAS.json', 'r') as file:\n    data = orjson.loads(file.read())\n\nimport shapely\nfrom shapely.geometry import shape\n")


# In[3]:


data[0]


# In[4]:


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
db_path = 'sqlite:////tmp/blog/spatialite_core.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# In[9]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[10]:


# Create the table
Base.metadata.create_all(engine)


# In[11]:


from tqdm import tqdm


# In[12]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    with  engine.connect() as conn:\n        for geojson in tqdm(data):\n            name = geojson["properties"]["classification"]["name"]\n            geometry = orjson.dumps(geojson["geometry"]).decode(\'ascii\')\n            res=conn.execute(\n                    text("SELECT SetSRID(GeomFromGeoJSON(:geom),-1);"),{"geom":geometry})\n')


# In[15]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    batch_size=5_000\n    polygons=[]\n    with  engine.connect() as conn:\n        tran = conn.begin()\n    \n        for geojson in tqdm(data):\n            name = geojson["properties"]["classification"]["name"]\n            geometry = orjson.dumps(geojson["geometry"]).decode(\'ascii\')\n        \n            polygons.append({\'name\':name,\'geom\':geometry})\n    \n            if len(polygons) == batch_size:\n                res=conn.execute(\n                    text("INSERT INTO geometries (name,geom) VALUES (:name,SetSRID(GeomFromGeoJSON(:geom),-1));"),polygons)\n    \n                polygons = []\n                tran.commit()\n                tran = conn.begin()\n    \n        if polygons:\n            res=conn.execute(\n                text("INSERT INTO geometries (name,geom) VALUES (:name,SetSRID(GeomFromGeoJSON(:geom),-1));"),polygons)\n        \n            polygons = []\n            tran.commit()\n')


# In[16]:


get_ipython().run_cell_magic('time', '', '#lets make sure insert worked as expected\nwith  engine.connect() as conn:\n    res=conn.execute(text("select count(geom) from geometries"))\n    nresults=res.fetchall()\n    print(nresults)\n')


# In[17]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    res=conn.execute(text("select AsGeoJSON(ST_Centroid(geom)) as centroid from geometries limit 1000"))\n    centroids=res.fetchall()\n')


# In[18]:


len(centroids)


# In[19]:


centroids


# In[ ]:




