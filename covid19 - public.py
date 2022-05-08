#!/usr/bin/env python
# coding: utf-8

# In[1]:


from io import StringIO
import pandas as pd
import requests
from datetime import datetime
import pyodbc
import math

url='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
s=requests.get(url).text

c=pd.read_csv(StringIO(s))

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


# In[2]:


#c.head()


# In[3]:


#c.tail()


# In[4]:


df = c[["continent","location","date","total_cases","total_deaths","people_fully_vaccinated","population"]]
#df.head()


# In[5]:


#df.tail()


# In[6]:


df1 = df[['continent','location','date']].fillna("")
df2 = df[['total_cases','total_deaths']].fillna(0)
df3 = df[['people_fully_vaccinated','population']].fillna(0)

#display(df1)
#display(df2)
df = pd.concat([df1, df2, df3], axis=1)
#df.head()


# In[7]:


server = ''
database = 'Covid19'
username = ''
password = ''   
driver= '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        
GetMaxDateQuery = """
    select 
        case 
            when max(date) is null then cast('2019-12-01' as date) 
            else max(date) 
        end as date 
    from covid19data"""

Returned_Date = pd.read_sql_query(GetMaxDateQuery, conn)
Max_Date = Returned_Date.iloc[0]['date']
print(Max_Date)      


# In[8]:


row_num = len(df)
print(row_num)


# In[9]:


start_date = pd.to_datetime(Max_Date)

df['date'] = pd.to_datetime(df['date'])
filtered_df = df[df["date"] > start_date]
filtered_df['date'] = pd.to_datetime(filtered_df['date'])

filtered_df = filtered_df.sort_values(by='date')

# row_num2 = len(filtered_df)
# print(row_num2)
# display(filtered_df.head())
# display(filtered_df.tail())


# In[10]:


row_num = len(filtered_df)
col_num = len(filtered_df.columns)
print(row_num)
print(col_num)
row_count = 0

cursor = conn.cursor()
#cursor.fast_executemany = True #Set fast_executemany  = True to increase inserting speed
# Insert Dataframe into SQL Server:
# for index, row in filtered_df.iterrows():
   
#      cursor.execute("INSERT INTO covid19data(continent,location,date,total_cases,new_cases,people_vaccinated,"+
#                      "people_fully_vaccinated,total_boosters,new_vaccinations,population) values(?,?,?,?,?,?,?,?,?,?)", 
#                      str(row.continent), str(row.location), row.date, 
#                      int(row.total_cases), int(row.new_cases), int(row.people_vaccinated), 
#                      int(row.people_fully_vaccinated), int(row.total_boosters), 
#                      int(row.new_vaccinations), int(row.population)
#                      )
    
for index, row in filtered_df.iterrows():
#for row in filtered_df.iterrows():
    row_count = int(row_count) + 1
    thousand_row = (row_count / 1000) % 1
    
    cursor.execute("INSERT INTO covid19data(continent,location,date,total_cases,total_deaths,"+
                     "people_fully_vaccinated,population) values(?,?,?,?,?,?,?)", 
                     str(row.continent), str(row.location), row.date, 
                     int(row.total_cases), int(row.total_deaths), int(row.people_fully_vaccinated), int(row.population)
                     )
    
    if float(thousand_row) == float(0):
        conn.commit()
        print("Committed at ",row_count)
    
    
conn.commit()
cursor.close()
now = datetime.now()
print("Insert has been done at ",now)


# In[ ]:




