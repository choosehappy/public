#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#docker run -p 5333:5432 --name postgis -e POSTGRES_HOST_AUTH_METHOD=trust  -d postgis/postgis


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


len(data[0])


# In[5]:


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# In[6]:


from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test2')#,echo=True)
print(engine.url)
try:
    create_database(engine.url)
    print("created")
except:
    print("errored")
    pass


# In[7]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# In[8]:


# Create the table
Base.metadata.create_all(engine)


# In[9]:


from tqdm import tqdm


# In[10]:


get_ipython().run_cell_magic('time', '', '## -- If you\'re curious, this test converts geojson to geom in the database *without* insertion\n## its a good baseline as this doesn\'t include any actual storing/etc/\n\nfor _ in range(12):\n    with  engine.connect() as conn:\n        for geojson in tqdm(data):\n            name = geojson["properties"]["classification"]["name"]\n            geometry = orjson.dumps(geojson["geometry"]).decode(\'ascii\')\n            res=conn.execute(\n                    text("SELECT ST_GeomFromGeoJSON(:geom);"),{"geom":geometry})\n')


# insert ~1 million in transaction batches 

# In[11]:


get_ipython().run_cell_magic('time', '', 'for _ in range(12):\n    batch_size=5_000\n    polygons=[]\n    with  engine.connect() as conn:\n        tran = conn.begin()\n    \n        for geojson in tqdm(data):\n            name = geojson["properties"]["classification"]["name"]\n            geometry = orjson.dumps(geojson["geometry"]).decode(\'ascii\')\n        \n            polygons.append({\'name\':name,\'geom\':geometry})\n    \n            if len(polygons) == batch_size:\n                res=conn.execute(\n                    text("INSERT INTO geometries (name,geom) VALUES (:name,ST_GeomFromGeoJSON(:geom));"),polygons)\n    \n                polygons = []\n                tran.commit()\n                tran = conn.begin()\n    \n        if polygons:\n            res=conn.execute(\n                text("INSERT INTO geometries (name,geom) VALUES (:name,ST_GeomFromGeoJSON(:geom));"),polygons)\n        \n            polygons = []\n            tran.commit()\n')


# In[12]:


get_ipython().run_cell_magic('time', '', '#lets make sure insert worked as expected\nwith  engine.connect() as conn:\n    res=conn.execute(text("select count(geom) from geometries"))\n    nresults=res.fetchall()\n    print(nresults)\n')


# In[14]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    res=conn.execute(text("select ST_AsGeoJSON(ST_Centroid(geom)) as centroid from geometries limit 1000"))\n    centroids=res.fetchall()\n')


# In[15]:


centroids


# In[ ]:




