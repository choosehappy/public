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

from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# Create the table
Base.metadata.create_all(engine)


from tqdm import tqdm

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select * from geometries")).fetchall()

len(results)

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,geom from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsGeoJSON(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsEWKB(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsEWKT(geom) from geometries")).fetchall()

results[0]

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_Centroid(geom) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsGeoJSON(ST_Centroid(geom)) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsEWKT(ST_Centroid(geom)) from geometries")).fetchall()

# %%time
with  engine.connect() as conn:
    results=conn.execute(text("select id,name,ST_AsEWKB(ST_Centroid(geom)) from geometries")).fetchall()


