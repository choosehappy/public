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
from sqlalchemy import create_engine, Column, String, Integer, func, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry 
from tqdm import tqdm
from shapely.wkt import dumps

import orjson


# +
# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# -

# Connect to the Spatialite database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# Create the table
Base.metadata.create_all(engine)


from tqdm import tqdm

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select * from geometries")).fetchall()

len(results)

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsGeoJSON(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsEWKB(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsEWKT(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_Centroid(geom) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsGeoJSON(ST_Centroid(geom)) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsEWKT(ST_Centroid(geom)) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,AsEWKB(ST_Centroid(geom)) from geometries")).fetchall()


