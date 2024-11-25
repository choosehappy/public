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


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# In[3]:


# Connect to the Spatialite database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# In[4]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[5]:


# Create the table
Base.metadata.create_all(engine)


# In[6]:


from tqdm import tqdm


# In[7]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select * from geometries")).fetchall()\n')


# In[8]:


len(results)


# In[9]:


results[0]


# In[10]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsGeoJSON(geom) from geometries")).fetchall()\n')


# In[11]:


results[0]


# In[12]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsEWKB(geom) from geometries")).fetchall()\n')


# In[13]:


results[0]


# In[14]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsEWKT(geom) from geometries")).fetchall()\n')


# In[15]:


results[0]


# In[16]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_Centroid(geom) from geometries")).fetchall()\n')


# In[17]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsGeoJSON(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[18]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsEWKT(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[19]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,AsEWKB(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[ ]:




