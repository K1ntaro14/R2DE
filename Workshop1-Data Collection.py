#Workshop on Google Colaboratory

#Install package for MySQL database connection
!pip install pymysql

#Config DB credential
import os
from typing import final

class Config:
  MYSQL_HOST =  ''
  MYSQL_PORT = 3306  # Default port MySQL
  MYSQL_USER = ''
  MYSQL_PASSWORD = ''
  MYSQL_DB = ''
  MYSQL_CHARSET = ''

#Connect to DB
import pymysql

connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

#list all tables
cursor = connection.cursor()
cursor.execute("show tables;")
tables = cursor.fetchall()
cursor.close()
print(tables) # Now we have 2 tables - audible_data and audible_transaction

#Query from audible_data
with connection.cursor() as cursor:
  cursor.execute("SELECT * FROM audible_data;")
  result = cursor.fetchall()

print("number of rows: ", len(result))
type(result)

#Convert to Pandas
import pandas as pd
audible_data = pd.DataFrame(result, index_col="Book_ID")
type(audible_data)

#Query from audible_transaction
sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)

#Join table: audible_transaction & audible_data
transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

#Get data from REST API
import requests
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()
print(type(result_conversion_rate))
assert isinstance(result_conversion_rate, dict)

conversion_rate = pd.DataFrame(result_conversion_rate) #Convert to Pandas

#Rename from index to date for join table with transaction table
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

transaction['date'] = transaction['timestamp']
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

#Join table to get conversion_rate
final_df = transaction.merge(conversion_rate, how="left", left_on= "date", right_on="date")

#str > float in Price column
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1) #Remove $
final_df["Price"] = final_df["Price"].astype(float)

#Creat column THBPrice
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]

final_df = final_df.drop("date", axis=1)

final_df.to_csv("output.csv", index=False)