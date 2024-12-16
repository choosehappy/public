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
# -


import ray

ray.init()

ray.cluster_resources()

# +
# %%time
with open('13_266069_040_003 L02 PAS.json', 'r') as file:
#with open('/mnt/c/research/kidney/15_26609_024_045 L03 PAS.json', 'r') as file:
    # Load the JSON data into a Python dictionary
    data = orjson.loads(file.read())

import shapely
from shapely.geometry import shape
# -

data[0]

# +
# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# +
from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/testpara')#,echo=True)

print(engine.url)
try:
    create_database(engine.url)
    print("created")
except:
    print("errored")
    pass


# -

# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# Create the table
Base.metadata.create_all(engine)


from tqdm import tqdm


@ray.remote
def bulk_insert(geojsons):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 
    engine = create_engine('postgresql://postgres@localhost:5333/testpara')#,echo=True)

    # Initialize Spatialite extension-
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
    
    try:

        # Create a base class for our declarative mapping
        Base = declarative_base()
        
        # Define your SQLAlchemy model
        class GeometryModel(Base):
            __tablename__ = 'geometries'
            id = Column(Integer, primary_key=True)
            name = Column(String)
            geom = Column(Geometry('POLYGON'))

        polygons=[]
        for geojson in geojsons:

            name = geojson["properties"]["classification"]["name"]
            wkt = shape(geojson["geometry"]).wkt
            
            polygons.append(GeometryModel(name=name,geom=wkt))
            
        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.bulk_save_objects(polygons)
            session.commit()

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# +
# %%time
futures = [] 
for _ in range(12):
    batch_size=5_000
    polygons=[]
    
    for geojson in tqdm(data):
        polygons.append(geojson)
    
        if len(polygons) == batch_size:
            futures.append(bulk_insert.remote(polygons))
            polygons=[]
    
    if polygons:
        futures.append(bulk_insert.remote(polygons))
    
for f in tqdm(futures):
    ray.get(f)
# -

# %%time
#lets make sure insert worked as expected
with  engine.connect() as conn:
    res=conn.execute(text("select count(geom) from geometries"))
    nresults=res.fetchall()
    print(nresults)

# %%time
with  engine.connect() as conn:
    res=conn.execute(text("select ST_AsGeoJSON(ST_centroid(geom))  from geometries limit 1000"))
    centroids=res.fetchall()

centroids[0:100]


