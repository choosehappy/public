#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


# In[3]:


import ray


# In[4]:


ray.init()


# In[5]:


ray.cluster_resources()


# In[6]:


db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path)#,echo=True)


# In[7]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[8]:


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


# In[9]:


# Create the table
Base.metadata.create_all(engine)


# In[10]:


get_ipython().run_cell_magic('time', '', '# -- orm approach\nfrom sqlalchemy.orm import Session\n\n# Getting the total number of rows\nwith Session(engine) as session:\n    total_rows = session.query(GeometryModel).count()\nprint(total_rows)    \n')


# ### baseline
# 

# In[21]:


@ray.remote
def query_baseline(random_index):
    return random_index


# In[25]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(100):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_baseline.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[26]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_baseline.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:





# In[ ]:





# In[ ]:


@ray.remote
def query_id_orm(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
    
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
            random_row = session.query(GeometryModel).filter_by(id=random_index).first()
            #--- approach 1 - return only id
            #return random_row.id
            #--- approach 2 - inline convert to shapely
            # geom = random_row.shapely_geom #force convert
            # return random_row.id
            #--- approach 3 - inline convert to shapely and compute area
            return random_row.shapely_geom.area
                

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_id_orm.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# #### test multiple

# In[12]:


@ray.remote
def query_id_orm_bulk(total_rows,num):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
    
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


# In[15]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nnum=80\nfor _ in range(128):\n    futures.append(query_id_orm_bulk.remote(total_rows,num))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:





# 

# In[ ]:


@ray.remote
def query_id_wkb(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
    
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

            # return random_row[0]
            return obj.area

    except Exception as inst:
        print(inst)
        pass
    finally:
        engine.dispose() ##might be needed? --- yes needed


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_id_wkb.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:


@ray.remote
def query_id_geojson(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
        
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
                SELECT id, AsGeoJSON(geom) as geom 
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


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_id_geojson.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:


ray.get(futures)


# In[ ]:


@ray.remote
def query_id_geojson_area(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
    
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
                SELECT id, AsGeoJSON(geom) as geom
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


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_id_geojson_area.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:


@ray.remote
def query_id_geojson_dynamic_centroid(random_index):
    from sqlalchemy import create_engine, Column, String, Integer, func, event, text
    from geoalchemy2 import Geometry 

    db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
    engine = create_engine(db_path)#,echo=True)

    # Initialize Spatialite extension
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        dbapi_connection.enable_load_extension(True)
        dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
        dbapi_connection.execute('SELECT InitSpatialMetaData(1);')
    
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
                SELECT id, AsGeoJSON(geom) as geom, AsGeoJSON(ST_Centroid(geom)) as centroid 
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


# In[ ]:


get_ipython().run_cell_magic('time', '', 'futures = [] \nfor _ in range(10_000):\n    random_index = random.randint(1, total_rows)\n    futures.append(query_id_geojson_dynamic_centroid.remote(random_index))\n    \nfor f in tqdm(futures):\n    ray.get(f)\n')


# In[ ]:





# In[ ]:





# In[ ]:





# ### below are spatial query tests

# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 500\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(ST_Centroid(geom)) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE ST_Intersects(\n            geom,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:


get_ipython().run_cell_magic('timeit', '', 'random_index = random.randint(1, total_rows)\n    \nhalf_bbox_size= 6_000\n# Step 3: Query for the specific row based on the random index\nwith engine.connect() as conn:\n    random_row = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(centroid) as centroid \n        FROM geometries \n        WHERE id = {random_index}\n        \'\'\')\n    ).fetchone()\n    \n    centroid_x,centroid_y=orjson.loads(random_row[1])[\'coordinates\']\n\n    bounding_box_polygons = conn.execute(\n        text(f\'\'\'\n        SELECT id, ST_AsGeoJSON(geom) as geom \n        FROM geometries \n        WHERE ST_Intersects(\n            centroid,\n            ST_MakeEnvelope(\n                {centroid_x - half_bbox_size}, {centroid_y - half_bbox_size},\n                {centroid_x + half_bbox_size}, {centroid_y + half_bbox_size},\n                4326\n            )\n        )\n        \'\'\')\n     ).fetchall()\n\n\n\nprint(len(bounding_box_polygons), end= " " )\n')


# In[ ]:




