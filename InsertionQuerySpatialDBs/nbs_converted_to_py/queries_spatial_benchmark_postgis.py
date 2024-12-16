# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
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
# -

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# +
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
# -

# Create the table
Base.metadata.create_all(engine)

# +
# %%time
# -- orm approach
from sqlalchemy.orm import Session

# Getting the total number of rows
with Session(engine) as session:
    total_rows = session.query(GeometryModel).count()
print(total_rows)    
# -

# #### Query Spatial Intersection 

# ## Core

# ### Geom

# Query Spatial Count-only Intersection 

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT count(geom) 
        FROM geometries 
        WHERE ST_Intersects(
            geom,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchone()



#print(npolys, end= " " )
# -

# id only

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT id
        FROM geometries 
        WHERE ST_Intersects(
            geom,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchall()



#print(npolys, end= " " )
# -

# return with geojson

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT ST_AsGeoJSON(geom)
        FROM geometries 
        WHERE ST_Intersects(
            geom,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchall()



#print(npolys, end= " " )
# -

# ### Centroid

# Query Spatial Count-only Intersection 

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT count(geom) 
        FROM geometries 
        WHERE ST_Intersects(
            centroid,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchone()



print(npolys, end= " " )
# -

s="(110316,) (89460,) (102888,) (97608,) (99024,) (117720,) (84444,) (128844,)"

# +
import numpy as np
s=s.replace("(","").replace(")","")
numbers = [int(num.strip()) for num in s.split(',') if num.strip()]

print(np.mean(numbers))
# -

# id only

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT id
        FROM geometries 
        WHERE ST_Intersects(
            centroid,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchall()



#print(npolys, end= " " )
# -

# return with geojson

# +
# %%timeit
#--- Core approach
## --- this is for counts only

random_index = random.randint(1, total_rows)
    
half_bbox_size= 6000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(centroid) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    
    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    npolys = conn.execute(
        text(f'''
        SELECT ST_AsGeoJSON(geom)
        FROM geometries 
        WHERE ST_Intersects(
            centroid,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchall()



#print(npolys, end= " " )
# -

# TO DELETE? #Count-only Centroid

# +
# # %%timeit
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
# -

# ## ORM

# Assuming you have a session
session = Session(bind=engine)

# ## GEOM 

# count only

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 6000

bounding_box_polygons = (
    session.query(
        func.count(GeometryModel.id))
    .filter(
        func.ST_Intersects(
            GeometryModel.geom,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -

# id only

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 6000

bounding_box_polygons = (
    session.query(
        GeometryModel.id)
    .filter(
        func.ST_Intersects(
            GeometryModel.geom,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -

# geom as geojson

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 6000

bounding_box_polygons = (
    session.query(
        func.ST_AsGeoJSON(GeometryModel.geom))
    .filter(
        func.ST_Intersects(
            GeometryModel.geom,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -

# ## Centroid

# count only

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 6000

bounding_box_polygons = (
    session.query(
        func.count(GeometryModel.id))
    .filter(
        func.ST_Intersects(
            GeometryModel.centroid,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -

# id only

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 6000

bounding_box_polygons = (
    session.query(
        GeometryModel.id)
    .filter(
        func.ST_Intersects(
            GeometryModel.centroid,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -

# geom as geojson

# +
# %%timeit
# Perform the query -- ORM approach
random_index = random.randint(1, total_rows)

random_row = (session.query(GeometryModel.id,func.ST_AsGeoJSON(func.ST_Centroid(GeometryModel.geom)))
            .filter(GeometryModel.id == random_index)
            .one_or_none())

centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

half_bbox_size= 100

bounding_box_polygons = (
    session.query(
        func.ST_AsGeoJSON(GeometryModel.geom))
    .filter(
        func.ST_Intersects(
            GeometryModel.centroid,
            func.ST_MakeEnvelope(
                centroid_x - half_bbox_size,
                centroid_y - half_bbox_size,
                centroid_x + half_bbox_size,
                centroid_y + half_bbox_size,
                4326  # SRID, adjust if needed
            )
        )
    )
    .all()
)

#print(len(bounding_box_polygons), end= " " )
# -


