#!/usr/bin/env python
# coding: utf-8

# In[1]:


import geoalchemy2


# In[2]:


geoalchemy2.__version__


# In[3]:


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


# In[4]:


from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


# In[5]:


from sqlalchemy_utils import database_exists, create_database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# In[6]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')


# In[7]:


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


# In[8]:


# Create the table
Base.metadata.create_all(engine)


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# -- orm approach\nfrom sqlalchemy.orm import Session\n\n# Getting the total number of rows\nwith Session(engine) as session:\n    total_rows = session.query(GeometryModel).count()\nprint(total_rows)    \n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# -- core approach\nfrom sqlalchemy import create_engine, MetaData, Table, select, func\nimport random\nwith  engine.connect() as conn:\n    total_rows = conn.execute(text("select count(*) from geometries;")).fetchone()[0]\nprint(total_rows)\n')


# In[14]:


### note the previous ORM cell has to be executed without "timeit" so that the total_rows are actually calculated and session is created


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\n# ORM approach\nrandom_index = random.randint(1, total_rows)\n\n# Retrieve the random row by ID and convert the geom to a Shapely object\nwith Session(engine) as session:\n    random_row = session.query(GeometryModel).filter_by(id=random_index).first()\n    #--- approach 1 - return only id\n    #print(random_row.id, end=" " )\n    #--- approach 2 - inline convert to shapely\n    # geom = random_row.shapely_geom #force convert\n    # print(random_row.id, end = " ") #but don\'t print polygon because too much info\n    #--- approach 3 - inline convert to shapely and compute area\n    print(random_row.shapely_geom.area, end = " ") ## uses property to compute dynamically\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# -- get the geom directly, and convert to shapely object via WKB\nrandom_index = random.randint(1, total_rows)\n\n#NAUCED DIFFERENCE HERE --- geom type in spatiliate is **not exactly** a WKB\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id,AsEWKB(geom)\n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    obj=shapely.wkb.loads(random_row[1])\n    \nprint(random_row[0],end=" ")\n#print(obj.area,end=" ") # lenght of the goejson -- shold be > 0\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\n# convert to geojson in the database, and return the string\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nprint(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# with compute centroid dynamic\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom\n        FROM geometries\n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nobj=shapely.from_geojson(random_row[1])\nprint(obj.area,end=" ") # lenght of the goejson -- shold be > 0\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '# with compute centroid dynamic\nrandom_index = random.randint(1, total_rows)\n\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\nprint(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0\n')


# In[ ]:





# In[ ]:





# In[ ]:





# ### below are spatial query tests

# In[ ]:


get_ipython().run_cell_magic('timeit', '-n 1 -r 100', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 1_000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n\n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n\n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '-n 1 -r 100', '\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 1_000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n\n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE geometries.ROWID IN (\n        SELECT ROWID\n        FROM SpatialIndex\n        WHERE f_table_name = \'geometries\'\n           AND search_frame =             BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            ))\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 100\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n\n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        ) AND geometries.ROWID IN (\n        SELECT ROWID\n        FROM SpatialIndex\n        WHERE f_table_name = \'geometries\'\n           AND search_frame =             BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            ))\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\nwith engine.connect() as conn:\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        ) AND geometries.ROWID IN (\n        SELECT ROWID\n        FROM SpatialIndex\n        WHERE f_table_name = \'geometries\'\n           AND search_frame =             BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            ))\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\nwith engine.connect() as conn:\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE geometries.ROWID IN (\n        SELECT ROWID\n        FROM SpatialIndex\n        WHERE f_table_name = \'geometries\'\n           AND search_frame =             BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            ))\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


a=text(f'''
SELECT id, AsGeoJSON(geom) as geom 
FROM geometries 
WHERE MbrIntersects(
    geom,
    BuildMBR(
        {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
        {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}
    )
) AND geometries.ROWID IN (
SELECT ROWID
FROM SpatialIndex
WHERE f_table_name = 'geometries'
   AND search_frame =             BuildMBR(
        {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
        {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}
    ))''')


# In[ ]:


print(a)


# In[ ]:


b=        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
        FROM geometries 
        WHERE geometries.ROWID IN (
        SELECT ROWID
        FROM SpatialIndex
        WHERE f_table_name = 'geometries'
           AND search_frame =             BuildMBR(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}
            ))
        ''')


# In[ ]:


print(b)


# In[ ]:


get_ipython().run_cell_magic('timeit', '', '\nwith engine.connect() as conn:\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


SELECT * 
FROM com2011 AS c, prov2011 AS p
WHERE ST_CoveredBy(c.geometry, p.geometry) = 1 
  AND nome_pro = 'AREZZO' AND c.ROWID IN (
    SELECT ROWID 
    FROM SpatialIndex
    WHERE f_table_name = 'com2011' 
        AND search_frame = p.geometry);


# In[ ]:


get_ipython().run_cell_magic('time', '', '\n\nrandom_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 1000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n\n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE MbrIntersects(\n            geom,\n            BuildMBR(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


db_path = 'sqlite:///spatialite_ray.db'
engine = create_engine(db_path, echo=True)  # Set echo=True to see SQL commands being executed
# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
#    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')

with engine.connect() as conn:
    bounding_box_polygons = conn.execute(
        text(f'''
        EXPLAIN QUERY PLAN SELECT id 
        FROM geometries 
        WHERE MbrIntersects(
            geom,
            BuildMBR(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size}
            )
        )
        ''')
     ).fetchall()


for p in bounding_box_polygons:
    print(p)


# In[ ]:





# In[ ]:





# In[ ]:


from sqlalchemy_utils import database_exists, create_database
db_path = 'sqlite:///spatialite_ray.db'
engine = create_engine(db_path, echo=True)  # Set echo=True to see SQL commands being executed
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
#    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[ ]:


Session = sessionmaker(bind=engine)
session = Session()


# In[ ]:


get_ipython().run_cell_magic('time', '', 'query = session.query(GeometryModel).filter(GeometryModel.geom.ST_Intersects(func.BuildMbr(\n            centroid_x - half_bbox_size,\n            centroid_y - half_bbox_size,\n            centroid_x + half_bbox_size,\n            centroid_y + half_bbox_size\n        )))\n\nresults=query.all()\nlen(results)\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6_000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:




