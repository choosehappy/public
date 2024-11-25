#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sqlalchemy import create_engine, Column, String, Integer, func, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry 
from tqdm import tqdm
from shapely.wkt import dumps

import orjson


# In[2]:


# Create a base class for our declarative mapping
Base = declarative_base()

# Define your SQLAlchemy model
class GeometryModel(Base):
    __tablename__ = 'geometries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry('POLYGON'))


# In[3]:


# Connect to the Spatialite database
db_path = 'sqlite:////tmp/blog/spatialite_ray.db'
engine = create_engine(db_path, echo=True)  # Set echo=True to see SQL commands being executed


# In[4]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    dbapi_connection.enable_load_extension(True)
    dbapi_connection.execute('SELECT load_extension("mod_spatialite")')
    dbapi_connection.execute('SELECT InitSpatialMetaData(1);')


# In[5]:


# Create the table
Base.metadata.create_all(engine)

# Start a session
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()


# In[6]:


get_ipython().run_cell_magic('time', '', 'results = session.query(GeometryModel).all()\n')


# In[7]:


len(results)


# In[8]:


get_ipython().run_cell_magic('time', '', '# Detach all results from the session\n# expunge_all() removes all objects from the session, making them detached. Detached objects are no longer tracked by the session, so changes made to them will not be automatically persisted to the database.\nsession.expunge_all()\n')


# In[9]:


get_ipython().run_cell_magic('time', '', '## -- Convert from EWKB to Shapely\nfrom shapely.geometry import shape\nfrom shapely import wkb\nfor r in tqdm(results):\n#    r.geom = wkb.loads(r.geom.data)\n    r.geom = wkb.loads(str(r.geom))\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_AsGeoJSON\nresults = session.query(ST_AsGeoJSON(GeometryModel.geom)).all()\n')


# In[ ]:


results[0]


# In[ ]:


# %%time
# from geoalchemy2.functions import ST_AsEWKT
# results = session.query(ST_AsEWKT(GeometryModel.geom)).all()


# In[ ]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_Centroid\nresults = session.query(ST_Centroid(GeometryModel.geom)).all()\n')


# In[ ]:


len(results)


# In[ ]:


results[0]


# In[ ]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_Centroid\nresults = session.query(ST_AsGeoJSON(ST_Centroid(GeometryModel.geom))).all()\n')


# In[ ]:


# %%time
results = session.query(GeometryModel.geom).limit(10).all()


# In[ ]:




