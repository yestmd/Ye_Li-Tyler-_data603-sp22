#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[2]:


stream_df = (spark.readStream.format('socket')
                             .option('host', 'localhost')
                             .option('port', 22223)
                             .load())

#json_df = stream_df.selectExpr("CAST(value AS STRING)")  #AS payload

writer = (
    stream_df.writeStream
           .queryName('askiss')
           .format('memory')
           .outputMode('append')
)

streamer = writer.start()


# In[3]:


import time

for _ in range(5):
    df = spark.sql("""
    SELECT *
    FROM askiss
    """)
    
    df.show(10)
    
    print(df)
    time.sleep(5)
    
streamer.awaitTermination(timeout=1300)
print('streaming done!')


# In[4]:


df.show(truncate = 300)


# In[5]:


from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_timestamp


# In[23]:


df1 = df.withColumn("time_", split(("value"), ",").getItem(0)) .withColumn("long", split(("value"), ",").getItem(2)) .withColumn("lat", split(("value"), ",").getItem(3)) # .show(truncate=300)


# In[24]:


df2=df1.withColumn("timestamp_", regexp_extract(df1.time_, "\\d+", 0)) .withColumn("longitude_", regexp_replace(df1.long, '["iss_position": {"longitude: "]', "")) .withColumn("latitude_", regexp_replace(df1.lat, '["latitude": , }}]', "")) 


# In[25]:


df3=df2.withColumn('longitude',df2['longitude_'].cast("float")) .withColumn('latitude',df2['latitude_'].cast("float")) .withColumn('timestamp', from_unixtime(col("timestamp_"),"MM-dd-yyyy HH:mm:ss")) 


# In[29]:


df3.select(col("timestamp")).show()


# In[30]:


df4=df3.withColumn("timestamp" ,to_timestamp(col("timestamp"),"MM-dd-yyyy HH:mm:ss"))


# In[31]:


df4.printSchema()


# In[32]:


pip install chart_studio


# In[33]:


pip install -U kaleido


# In[34]:


import chart_studio.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)


# In[35]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


# In[36]:


df4_pd=df4.toPandas()


# In[37]:


# df4_pd.to_csv("result.csv")
# df4_pd=pd.read_csv("result.csv")


# In[38]:


plt.figure(figsize =(12,8))
fig=px.scatter_geo(df4_pd, lat=df4_pd["latitude"], lon=df4_pd["longitude"], hover_name='timestamp')
#plt.title(df4_pd["timestamp"].min() + " to " + df4_pd["timestamp"].max())
fig.update_layout(title=str(df4_pd["timestamp"].min()) + " to " + str(df4_pd["timestamp"].max()))
fig.show()
fig.write_image("hw06_map.png")


# In[ ]:




