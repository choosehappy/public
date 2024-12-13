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

# In[65]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT count(geom) \n        FROM geometries \n        WHERE ST_Intersects(\n            geom,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchone()\n\n\n\n#print(npolys, end= " " )\n')


# id only

# In[70]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT id\n        FROM geometries \n        WHERE ST_Intersects(\n            geom,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# return with geojson

# In[75]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT ST_AsGeoJSON(geom)\n        FROM geometries \n        WHERE ST_Intersects(\n            geom,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# ### Centroid

# Query Spatial Count-only Intersection 

# In[76]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT count(geom) \n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchone()\n\n\n\nprint(npolys, end= " " )\n')


# In[48]:


s="(110316,) (89460,) (102888,) (97608,) (99024,) (117720,) (84444,) (128844,)"


# In[49]:


import numpy as np
s=s.replace("(","").replace(")","")
numbers = [int(num.strip()) for num in s.split(',') if num.strip()]

print(np.mean(numbers))


# id only

# In[54]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT id\n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# return with geojson

# In[59]:


get_ipython().run_cell_magic('timeit', '', '#--- Core approach\n## --- this is for counts only\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    npolys = conn.execute(\n        text(f\'\'\'\n        SELECT ST_AsGeoJSON(geom)\n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\n#print(npolys, end= " " )\n')


# TO DELETE? #Count-only Centroid

# In[ ]:


# %%timeit
# #--- Core approach
# ## --- this is for counts only

# random_index = random.randint(1, total_rows)
    
# half_bbox_size= 6000
# # Step 3: Query for the specific row based on the random index
# with engine.connect() as conn:
#     random_row = conn.execute(
#         text(f'''
#         SELECT id, ST_AsGeoJSON(centroid) as centroid 
#         FROM geometries 
#         WHERE id = {random_index}
#         ''')
#     ).fetchone()
    
#     centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

#     npolys = conn.execute(
#         text(f'''
#         SELECT ST_AsGeoJSON(ST_Centroidgeom)
#         FROM geometries 
#         WHERE ST_Intersects(
#             centroid,
#             ST_MakeEnvelope(
#                 {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
#                 {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
#                 4326
#             )
#         )
#         ''')
#      ).fetchall()



# #print(npolys, end= " " )


# ## ORM

# In[77]:


# Assuming you have a session
session = Session(bind=engine)


# ## GEOM 

# count only

# In[86]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.count(GeometryModel.id))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# id only

# In[93]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        GeometryModel.id)\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# geom as geojson

# In[100]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.ST_AsGeoJSON(GeometryModel.geom))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.geom,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# ## Centroid

# count only

# In[110]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        func.count(GeometryModel.id))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# id only

# In[114]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 6000\n\nbounding_box_polygons = (\n    session.query(\n        GeometryModel.id)\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# geom as geojson

# In[119]:


get_ipython().run_cell_magic('timeit', '', '# Perform the query -- ORM approach\nrandom_index = random.randint(1, total_rows)\n\nrandom_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))\n            .filter(GeometryModel.id == random_index)\n            .one_or_none())\n\ncentroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\nhalf_bbox_size= 100\n\nbounding_box_polygons = (\n    session.query(\n        func.ST_AsGeoJSON(GeometryModel.geom))\n    .filter(\n        func.ST_Intersects(\n            GeometryModel.centroid,\n            func.ST_MakeEnvelope(\n                centroid_x - half_bbox_size,\n                centroid_y - half_bbox_size,\n                centroid_x + half_bbox_size,\n                centroid_y + half_bbox_size,\n                4326  # SRID, adjust if needed\n            )\n        )\n    )\n    .all()\n)\n\n#print(len(bounding_box_polygons), end= " " )\n')


# In[ ]:




