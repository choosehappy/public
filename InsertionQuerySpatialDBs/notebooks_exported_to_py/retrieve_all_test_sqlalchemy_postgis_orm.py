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


from sqlalchemy_utils import database_exists, create_database
engine = create_engine('postgresql://postgres@localhost:5333/test')#,echo=True)


# In[4]:


# Initialize Spatialite extension
@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


# In[5]:


# Create the table
Base.metadata.create_all(engine)

# Start a session
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()


# In[ ]:


get_ipython().run_cell_magic('time', '', 'results = session.query(GeometryModel).all()\n')


# In[41]:


len(results)


# In[42]:


get_ipython().run_cell_magic('time', '', '# Detach all results from the session\n# expunge_all() removes all objects from the session, making them detached. Detached objects are no longer tracked by the session, so changes made to them will not be automatically persisted to the database.\nsession.expunge_all()\n')


# In[43]:


get_ipython().run_cell_magic('time', '', '## -- Convert from EWKB to Shapely\nfrom shapely.geometry import shape\nfrom shapely import wkb\nfor r in tqdm(results):\n    r.geom = wkb.loads(str(r.geom)) #note that this is different than the spatialite version\n')


# In[ ]:





# In[ ]:





# In[10]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_AsGeoJSON\nresults = session.query(ST_AsGeoJSON(GeometryModel.geom)).all()\n')


# In[11]:


results[0]


# In[12]:


# %%time
# from geoalchemy2.functions import ST_AsEWKT
# results = session.query(ST_AsEWKT(GeometryModel.geom)).all()


# In[13]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_Centroid\nresults = session.query(ST_Centroid(GeometryModel.geom)).all()\n')


# In[14]:


len(results)


# In[15]:


results[0]


# In[16]:


get_ipython().run_cell_magic('time', '', 'from geoalchemy2.functions import ST_Centroid\nresults = session.query(ST_AsGeoJSON(ST_Centroid(GeometryModel.geom))).all()\n')


# In[17]:


# %%time
results = session.query(GeometryModel.geom).limit(10).all()


# In[ ]:




