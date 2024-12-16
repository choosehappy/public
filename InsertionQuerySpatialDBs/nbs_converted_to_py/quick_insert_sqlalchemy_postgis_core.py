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
#docker run -p 5333:5432 --name postgis -e POSTGRES_HOST_AUTH_METHOD=trust  -d postgis/postgis

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
# %%time
with open('13_266069_040_003 L02 PAS.json', 'r') as file:
    data = orjson.loads(file.read())

import shapely
from shapely.geometry import shape
# -

data[0]

len(data[0])

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
engine = create_engine('postgresql://postgres@localhost:5333/test2')#,echo=True)
print(engine.url)
try:
    create_database(engine.url)
    print("created")
except:
    print("errored")
    pass


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# Create the table
Base.metadata.create_all(engine)


from tqdm import tqdm

# +
# %%time
## -- If you're curious, this test converts geojson to geom in the database *without* insertion
## its a good baseline as this doesn't include any actual storing/etc/

for _ in range(12):
    with  engine.connect() as conn:
        for geojson in tqdm(data):
            name = geojson["properties"]["classification"]["name"]
            geometry = orjson.dumps(geojson["geometry"]).decode('ascii')
            res=conn.execute(
                    text("SELECT ST_GeomFromGeoJSON(:geom);"),{"geom":geometry})
# -

# insert ~1 million in transaction batches 

# %%time
for _ in range(12):
    batch_size=5_000
    polygons=[]
    with  engine.connect() as conn:
        tran = conn.begin()
    
        for geojson in tqdm(data):
            name = geojson["properties"]["classification"]["name"]
            geometry = orjson.dumps(geojson["geometry"]).decode('ascii')
        
            polygons.append({'name':name,'geom':geometry})
    
            if len(polygons) == batch_size:
                res=conn.execute(
                    text("INSERT INTO geometries (name,geom) VALUES (:name,ST_GeomFromGeoJSON(:geom));"),polygons)
    
                polygons = []
                tran.commit()
                tran = conn.begin()
    
        if polygons:
            res=conn.execute(
                text("INSERT INTO geometries (name,geom) VALUES (:name,ST_GeomFromGeoJSON(:geom));"),polygons)
        
            polygons = []
            tran.commit()

# %%time
#lets make sure insert worked as expected
with  engine.connect() as conn:
    res=conn.execute(text("select count(geom) from geometries"))
    nresults=res.fetchall()
    print(nresults)

# %%time
with  engine.connect() as conn:
    res=conn.execute(text("select ST_AsGeoJSON(ST_Centroid(geom)) as centroid from geometries limit 1000"))
    centroids=res.fetchall()

centroids


