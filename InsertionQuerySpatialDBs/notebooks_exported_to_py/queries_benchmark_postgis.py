#!/usr/bin/env python
# coding: utf-8

# In[1]:


import orjson

from sqlalchemy import create_engine, Column, String, Integer, func, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry 
from tqdm import tqdm
from shapely.wkt import dumps
import shapely
from shapely.wkb import loads as load_wkb
import random


# In[2]:


from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


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


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))

    @property
    def shapely_geom(self):
        return load_wkb(self.geom.desc) if self.geom else None


# In[6]:


# Create the table
Base.metadata.create_all(engine)


# In[8]:


get_ipython().run_cell_magic('timeit', '', '# -- orm approach\nfrom sqlalchemy.orm import Session\n\n# Getting the total number of rows\nwith Session(engine) as session:\n    total_rows = session.query(GeometryModel).count()\nprint(total_rows)    \n')


# In[9]:


get_ipython().run_cell_magic('timeit', '', 'from sqlalchemy import create_engine, MetaData, Table, select, func\nimport random\nwith  engine.connect() as conn:\n    total_rows = conn.execute(text("select count(*) from geometries;")).fetchone()[0]\nprint(total_rows)\n')


# In[12]:


get_ipython().run_cell_magic('timeit', '', '\n# ORM approach\nrandom_index = random.randint(1, total_rows)\n\n# Retrieve the random row by ID and convert the geom to a Shapely object\nwith Session(engine) as session:\n    random_row = session.query(GeometryModel).filter_by(id=random_index).first()\n    #--- approach 1 - return only id\n    #print(random_row.id, end=" " )\n    #--- approach 2 - inline convert to shapely\n    # geom = random_row.shapely_geom #force convert\n    # print(random_row.id, end = " ") #but don\'t print polygon because too much info\n    #--- approach 3 - inline convert to shapely and compute area\n    print(random_row.shapely_geom.area, end = " ") ## uses property to compute dynamically\n')


# In[14]:


get_ipython().run_cell_magic('timeit', '', '# -- get the geom directly, and convert to shapely object via WKB\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id,geom\n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    obj=shapely.wkb.loads(random_row[1])\n#print(random_row[0],end=" ") # lenght of the goejson -- shold be > 0\nprint(obj.area,end=" ") # lenght of the goejson -- shold be > 0\n')


# In[15]:


get_ipython().run_cell_magic('timeit', '', '\n# convert to geojson in the database, and return the string\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nprint(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0\n')


# In[16]:


get_ipython().run_cell_magic('timeit', '', '# with compute centroid dynamic\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom\n        FROM geometries\n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nobj=shapely.from_geojson(random_row[1])\nprint(obj.area,end=" ") # lenght of the goejson -- shold be > 0\n')


# In[17]:


get_ipython().run_cell_magic('timeit', '', '# with compute centroid dynamic\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom, ST_AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nprint(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0\n')


# In[ ]:





# In[ ]:





# In[ ]:





# ### below are spatial query tests

# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 500\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE ST_Intersects(\n            geom,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6_000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:




