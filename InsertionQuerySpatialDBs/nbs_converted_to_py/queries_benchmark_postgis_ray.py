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

import ray

ray.init()

ray.cluster_resources()

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

@ray.remote
def query_id_orm(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None


        with Session(engine) as session:
            #random_row = session.query(GeometryModel).filter_by(id=random_index).first()
            random_row = session.query(GeometryModel).first()
            #--- approach 1 - return only id
            return random_row.id
            #--- approach 2 - inline convert to shapely
            # geom = random_row.shapely_geom #force convert
            # return random_row.id
            #--- approach 3 - inline convert to shapely and compute area
            #return random_row.shapely_geom.area
                

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed

# +
# %%time
futures = [] 
for _ in range(10_000):
    random_index = random.randint(1, total_rows)
    futures.append(query_id_orm.remote(random_index))
    
for f in tqdm(futures):
    ray.get(f)


# -

# ### test multiple

@ray.remote
def query_id_orm_bulk(total_rows,num):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None

        retval=[]
        with Session(engine) as session:
            for _ in range(num):
                random_index = random.randint(1, total_rows)
                random_row = session.query(GeometryModel).filter_by(id=random_index).first()
                retval.append(random_row.id)
        #     #--- approach 1 - return only id
        #     return random_row.id
        #     #--- approach 2 - inline convert to shapely
        #     # geom = random_row.shapely_geom #force convert
        #     # return random_row.id
        #     #--- approach 3 - inline convert to shapely and compute area
        #     #return random_row.shapely_geom.area
        return retval

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# +
# %%time
futures = [] 
num=80
for _ in range(128):
    futures.append(query_id_orm_bulk.remote(total_rows,num))
    
for f in tqdm(futures):
    ray.get(f)
# -

len(sum(ray.get(futures),[]))

ray.get(futures)



@ray.remote
def query_id_wkb(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None

        with engine.connect() as conn:
            random_row = conn.execute(
                text(f'''
                SELECT id,geom
                FROM geometries 
                WHERE id = {random_index}
                ''')
            ).fetchone()
            obj=shapely.wkb.loads(random_row[1])

            return random_row[0]
            #return obj.area

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# +
# %%time
futures = [] 
for _ in range(10_000):
    random_index = random.randint(1, total_rows)
    futures.append(query_id_wkb.remote(random_index))
    
for f in tqdm(futures):
    ray.get(f)


# -

@ray.remote
def query_id_geojson(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None

        with engine.connect() as conn:
            random_row = conn.execute(
                text(f'''
                SELECT id, ST_AsGeoJSON(geom) as geom 
                FROM geometries 
                WHERE id = {random_index}
                ''')
            ).fetchone()
            return len(random_row[1])
            


    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed

# +
# %%time
futures = [] 
for _ in range(10_000):
    random_index = random.randint(1, total_rows)
    futures.append(query_id_geojson.remote(random_index))
    
for f in tqdm(futures):
    ray.get(f)
# -

ray.get(futures)


@ray.remote
def query_id_geojson_area(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None
        
        with engine.connect() as conn:
            random_row = conn.execute(
                text(f'''
                SELECT id, ST_AsGeoJSON(geom) as geom
                FROM geometries
                WHERE id = {random_index}
                ''')
            ).fetchone()
        obj=shapely.from_geojson(random_row[1])
        return obj.area
            


    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed

# +
# %%time
futures = [] 
for _ in range(10_000):
    random_index = random.randint(1, total_rows)
    futures.append(query_id_geojson_area.remote(random_index))
    
for f in tqdm(futures):
    ray.get(f)


# -

@ray.remote
def query_id_geojson_dynamic_centroid(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)

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
        
            @property
            def shapely_geom(self):
                return load_wkb(self.geom.desc) if self.geom else None
        
        with engine.connect() as conn:
            random_row = conn.execute(
                text(f'''
                SELECT id, ST_AsGeoJSON(geom) as geom, ST_AsGeoJSON(ST_Centroid(geom)) as centroid 
                FROM geometries 
                WHERE id = {random_index}
                ''')
            ).fetchone()
        return len(random_row[1])
            


    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed

# +
# %%time
futures = [] 
for _ in range(10_000):
    random_index = random.randint(1, total_rows)
    futures.append(query_id_geojson_dynamic_centroid.remote(random_index))
    
for f in tqdm(futures):
    ray.get(f)
# -







# ### below are spatial query tests

# +
# %%timeit
random_index = random.randint(1, total_rows)
    
half_bbox_size= 500
# Step 3: Query for the specific row based on the random index
with engine.connect() as conn:
    random_row = conn.execute(
        text(f'''
        SELECT id, ST_AsGeoJSON(ST_Centroid(geom)) as centroid 
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
            geom,
            ST_MakeEnvelope(
                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},
                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},
                4326
            )
        )
        ''')
     ).fetchall()



print(len(bounding_box_polygons), end= " " )

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




