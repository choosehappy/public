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


db_path = 'sqlite:////tmp/blog/spatialite_core_orm.db'
engine = create_engine(db_path)#,echo=True)


# In[4]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[5]:


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))
    centroid = Column(Geometry('POINT'))


    @property
    def shapely_geom(self):
        return load_wkb(self.geom.desc) if self.geom else None


# In[6]:


# Create the table
Base.metadata.create_all(engine)


# In[7]:


get_ipython().run_cell_magic('time', '', '# -- orm approach\nfrom sqlalchemy.orm import Session\n\n# Getting the total number of rows\nwith Session(engine) as session:\n    total_rows = session.query(GeometryModel).count()\nprint(total_rows)    \n')


# #### Query Spatial Intersection 

# ## Core

# ### Geom

# Query Spatial Count-only Intersection 

# In[8]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT count(geom) \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}                \n            )\n        )\n        \'\'\')\n     ).fetchone()\n\n\n\nprint(npolys, end= " " )\n')


# id only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT id\n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# return with geojson

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT AsGeoJSON(geom)\n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# ### Centroid

# Query Spatial Count-only Intersection 

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT count(geom) \n        FROM geometries \n        WHERE MbrIntersects(\n            centroid,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n                \n            )\n        )\n        \'\'\')\n     ).fetchone()\n\n\n\n#print(npolys, end= " " )\n')


# In[ ]:


s="(110316,) (89460,) (102888,) (97608,) (99024,) (117720,) (84444,) (128844,)"


# In[ ]:


import numpy as np
s=s.replace("(","").replace(")","")
numbers = [int(num.strip()) for num in s.split(',') if num.strip()]

print(np.mean(numbers))


# id only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT id\n        FROM geometries \n        WHERE MbrIntersects(\n            centroid,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# return with geojson

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT AsGeoJSON(geom)\n        FROM geometries \n        WHERE MbrIntersects(\n            centroid,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# ## ORM

# In[ ]:


# Assuming you have a session
session = Session(bind=engine)


# ## GEOM 

# count only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.count(GeometryModel.id))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# id only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        GeometryModel.id)\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# geom as geojson

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.ST_AsGeoJSON(GeometryModel.geom))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# ## Centroid

# count only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.count(GeometryModel.id))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# id only

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        GeometryModel.id)\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# geom as geojson

# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.ST_AsGeoJSON(GeometryModel.geom))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.BuildMBR(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# In[ ]:




