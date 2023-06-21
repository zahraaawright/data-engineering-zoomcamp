#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyarrow.parquet as pq


# In[2]:


output_name = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


# In[3]:


file_name = 'yellow_tripdata_2021-01.parquet'


# In[4]:


parquet_file = pq.ParquetFile(file_name)
parquet_size = parquet_file.metadata.num_rows


# In[5]:


table_name="yellow_taxi_data"


# In[6]:


from sqlalchemy import create_engine


# In[7]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[8]:


# Clear table if exists
pq.read_table(file_name).to_pandas().head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


# In[9]:


# default (and max) batch size
index = 65536


# In[10]:


from time import time


# In[11]:


for i in parquet_file.iter_batches(use_threads=True):
    t_start = time()
    print(f'Ingesting {index} out of {parquet_size} rows ({index / parquet_size:.0%})')
    i.to_pandas().to_sql(name=table_name, con=engine, if_exists='append')
    index += 65536
    t_end = time()
    print(f'\t- it took %.1f seconds' % (t_end - t_start))


# In[ ]:




