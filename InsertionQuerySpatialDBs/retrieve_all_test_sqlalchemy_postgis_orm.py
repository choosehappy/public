# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
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
    r.geom = wkb.loads(str(r.geom)) #note that this is different than the spatialite version





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


