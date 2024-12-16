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

# +
# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    centroid = Column(Geometry('POINT'))
    geom = Column(Geometry('POLYGON'))


# -

from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test22')#,echo=True)
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
                # res=conn.execute(
                #     text("INSERT INTO geometries (name,geom,centroid) VALUES (:name,ST_GeomFromGeoJSON(:geom),ST_Centroid(ST_GeomFromGeoJSON(:geom)));"),polygons)

                #optimized query
                res=conn.execute(
                    text("INSERT INTO geometries (name, geom, centroid) SELECT :name, geom, ST_Centroid(geom)  FROM (SELECT ST_GeomFromGeoJSON(:geom) AS geom) AS subquery;"),polygons)

                polygons = []
                tran.commit()
                tran = conn.begin()
    
        if polygons:
            # res=conn.execute(
            #     text("INSERT INTO geometries (name,geom,centroid) VALUES (:name,ST_GeomFromGeoJSON(:geom),ST_Centroid(ST_GeomFromGeoJSON(:geom)));"),polygons)

            #optimized query
            res=conn.execute(
                    text("INSERT INTO geometries (name, geom, centroid) SELECT :name, geom, ST_Centroid(geom)  FROM (SELECT ST_GeomFromGeoJSON(:geom) AS geom) AS subquery;"),polygons)

            
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
    res=conn.execute(text("select ST_AsGeoJSON(centroid) as centroid from geometries limit 1000"))
    centroids=res.fetchall()

centroids[0:100]


