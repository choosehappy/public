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
engine = create_engine(db_path, echo=True)  # Set echo=True to see SQL commands being executed


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# +
# Create the table
Base.metadata.create_all(engine)

# Start a session
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
# -

# %%time
results = session.query(GeometryModel).all()

len(results)

# %%time
# Detach all results from the session
# expunge_all() removes all objects from the session, making them detached. Detached objects are no longer tracked by the session, so changes made to them will not be automatically persisted to the database.
session.expunge_all()

# %%time
## -- Convert from EWKB to Shapely
from shapely.geometry import shape
from shapely import wkb
for r in tqdm(results):
#    r.geom = wkb.loads(r.geom.data)
    r.geom = wkb.loads(str(r.geom))

# %%time
from geoalchemy2.functions import ST_AsGeoJSON
results = session.query(ST_AsGeoJSON(GeometryModel.geom)).all()

results[0]

# +
# # %%time
# from geoalchemy2.functions import ST_AsEWKT
# results = session.query(ST_AsEWKT(GeometryModel.geom)).all()
# -

# %%time
from geoalchemy2.functions import ST_Centroid
results = session.query(ST_Centroid(GeometryModel.geom)).all()

len(results)

results[0]

# %%time
from geoalchemy2.functions import ST_Centroid
results = session.query(ST_AsGeoJSON(ST_Centroid(GeometryModel.geom))).all()

# # %%time
results = session.query(GeometryModel.geom).limit(10).all()


