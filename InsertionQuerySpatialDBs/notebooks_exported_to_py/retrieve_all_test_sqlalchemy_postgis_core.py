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


from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)


# In[4]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# In[5]:


# Create the table
Base.metadata.create_all(engine)


# In[6]:


from tqdm import tqdm


# In[7]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select * from geometries")).fetchall()\n')


# In[8]:


len(results)


# In[11]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,geom from geometries")).fetchall()\n')


# In[12]:


results[0]


# In[13]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsGeoJSON(geom) from geometries")).fetchall()\n')


# In[14]:


results[0]


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsEWKB(geom) from geometries")).fetchall()\n')


# In[ ]:


results[0]


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsEWKT(geom) from geometries")).fetchall()\n')


# In[ ]:


results[0]


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_Centroid(geom) from geometries")).fetchall()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsGeoJSON(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsEWKT(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with  engine.connect() as conn:\n    results=conn.execute(text("select id,name,ST_AsEWKB(ST_Centroid(geom)) from geometries")).fetchall()\n')


# In[ ]:




