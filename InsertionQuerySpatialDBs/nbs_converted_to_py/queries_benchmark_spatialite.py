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

import geoalchemy2

geoalchemy2.__version__

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
# -

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sqlalchemy_utils import database_exists, create_database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')



# +
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


# -

# Create the table
Base.metadata.create_all(engine)

# +
# %%timeit
# -- orm approach
from sqlalchemy.orm import Session

# Getting the total number of rows
with Session(engine) as session:
    total_rows = session.query(GeometryModel).count()
print(total_rows)    
# -

# %%timeit
# -- core approach
from sqlalchemy import create_engine, MetaData, Table, select, func
import random
with  engine.connect() as conn:
    total_rows = conn.execute(text("select count(*) from geometries;")).fetchone()[0]
print(total_rows)


# +
### note the previous ORM cell has to be executed without "timeit" so that the total_rows are actually calculated and session is created

# +
# %%timeit

# ORM approach
random_index = random.randint(1, total_rows)

# Retrieve the random row by ID and convert the geom to a Shapely object
with Session(engine) as session:
    random_row = session.query(GeometryModel).filter_by(id=random_index).first()
    #--- approach 1 - return only id
    #print(random_row.id, end=" " )
    #--- approach 2 - inline convert to shapely
    # geom = random_row.shapely_geom #force convert
    # print(random_row.id, end = " ") #but don't print polygon because too much info
    #--- approach 3 - inline convert to shapely and compute area
    print(random_row.shapely_geom.area, end = " ") ## uses property to compute dynamically


# +
# %%timeit
# -- get the geom directly, and convert to shapely object via WKB
random_index = random.randint(1, total_rows)

#NAUCED DIFFERENCE HERE --- geom type in spatiliate is **not exactly** a WKB
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id,AsEWKB(geom)
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    obj=shapely.wkb.loads(random_row[1])
    
print(random_row[0],end=" ")
#print(obj.area,end=" ") # lenght of the goejson -- shold be > 0

# +
# %%timeit

# convert to geojson in the database, and return the string
random_index = random.randint(1, total_rows)

with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
print(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0

# +
# %%timeit
# with compute centroid dynamic
random_index = random.randint(1, total_rows)

with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom
        FROM geometries
        WHERE id = {random_index}
        ''')
    ).fetchone()
obj=shapely.from_geojson(random_row[1])
print(obj.area,end=" ") # lenght of the goejson -- shold be > 0

# +
# %%timeit
# with compute centroid dynamic
random_index = random.randint(1, total_rows)

with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
print(len(random_row[1]),end=" ") # lenght of the goejson -- shold be > 0
# -







# ### below are spatial query tests

# +
# %%timeit -n 1 -r 100
random_index = random.randint(1, total_rows)
    
half_bbox_size= 1_000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    

    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    bounding_box_polygons = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
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

print(len(bounding_box_polygons), end= " " )

# +
# %%timeit
random_index = random.randint(1, total_rows)
    
half_bbox_size= 100
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    

    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    bounding_box_polygons = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
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

print(len(bounding_box_polygons), end= " " )

# +
# %%timeit -n 1 -r 100

random_index = random.randint(1, total_rows)
    
half_bbox_size= 1_000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    

    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    bounding_box_polygons = conn.execute(
        text(f'''
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
     ).fetchall()

print(len(bounding_box_polygons), end= " " )

# +
# %%timeit

random_index = random.randint(1, total_rows)
    
half_bbox_size= 100
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    

    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    bounding_box_polygons = conn.execute(
        text(f'''
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
            ))
        ''')
     ).fetchall()

print(len(bounding_box_polygons), end= " " )

# +
# %%timeit

with engine.connect() as conn:

    bounding_box_polygons = conn.execute(
        text(f'''
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
            ))
        ''')
     ).fetchall()

print(len(bounding_box_polygons), end= " " )

# +
# %%timeit

with engine.connect() as conn:

    bounding_box_polygons = conn.execute(
        text(f'''
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
     ).fetchall()

print(len(bounding_box_polygons), end= " " )
# -

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

print(a)

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

print(b)

# +
# %%timeit

with engine.connect() as conn:

    bounding_box_polygons = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
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

print(len(bounding_box_polygons), end= " " )
# -

SELECT * 
FROM com2011 AS c, prov2011 AS p
WHERE ST_CoveredBy(c.geometry, p.geometry) = 1 
  AND nome_pro = 'AREZZO' AND c.ROWID IN (
    SELECT ROWID 
    FROM SpatialIndex
    WHERE f_table_name = 'com2011' 
        AND search_frame = p.geometry);

# +
# %%time


random_index = random.randint(1, total_rows)
    
half_bbox_size= 1000
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(ST_Centroid(geom)) as centroid 
        FROM geometries 
        WHERE id = {random_index}
        ''')
    ).fetchone()
    

    centroid_x,centroid_y=orjson.loads(random_row[1])['coordinates']

    bounding_box_polygons = conn.execute(
        text(f'''
        SELECT id, AsGeoJSON(geom) as geom 
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

print(len(bounding_box_polygons), end= " " )
# -







# +
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
# -





from sqlalchemy_utils import database_exists, create_database
db_path = 'sqlite:///spatialite_ray.db'
engine = create_engine(db_path, echo=True)  # Set echo=True to see SQL commands being executed
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
#    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


Session = sessionmaker(bind=engine)
session = Session()

# +
# %%time
query = session.query(GeometryModel).filter(GeometryModel.geom.ST_Intersects(func.BuildMbr(
            centroid_x - half_bbox_size,
            centroid_y - half_bbox_size,
            centroid_x + half_bbox_size,
            centroid_y + half_bbox_size
        )))

results=query.all()
len(results)


# +
# %%timeit
random_index = random.randint(1, total_rows)
    
half_bbox_size= 6_000
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

    bounding_box_polygons = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(geom) as geom 
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



print(len(bounding_box_polygons), end= " " )
# -


