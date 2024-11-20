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


# -

# Connect to the Spatialite database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=False)  # Set echo=True to see SQL commands being executed


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute("PRAGMA timeout= 1000;")    


# Create the table
Base.metadata.create_all(engine)


from tqdm import tqdm


@ray.remote
def bulk_insert(polygons):
    engine = create_engine('sqlite:////tmp/blog/spatialite_ray.db')#,echo=True)
    
    try:
        # Initialize Spatialite extension
        @event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            dbapi_connection.enable_load_extension(True)
            dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
            dbapi_connection.execute("PRAGMA busy_timeout= 10000000;")   
    
        with engine.connect() as conn:
            with conn.begin() as tran:
                res=conn.execute(text("INSERT INTO geometries (name,geom) VALUES (:name,GeomFromGeoJSON(:geom));"),polygons)
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
        name = geojson["properties"]["classification"]["name"]
        geometry = orjson.dumps(geojson["geometry"],).decode("utf-8")
    
        polygons.append({'name':name,'geom':geometry})
    
        
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
    res=conn.execute(text("select AsGeoJSON(ST_centroid(geom)) from geometries limit 1000"))
    centroids=res.fetchall()

centroids[0:100]


