import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
from pretty_html_table import build_table
df = pd.read_csv("hdfs-audit.log", skiprows=1, names=["Date", "Time", "LOGLEVEL", "system",  "permission",  "user",  "Authentication",  "IP", "Operation", "Source", "Destination", "Permission", "Protocol"], delim_whitespace=True)
#df.columns = ["sno", "Date", "Time", "LOGLEVEL", "system",  "permission",  "user",  "Authentication",  "IP", "Operation", "Source", "Destination", "Permission", "Protocol"]
#df = pd.read_csv("hdfs-audit.log")
#print(df.shape)
#df_time = df['Time'].dt.strftime("%H:%M:%S,%f")
df["Time"] = pd.to_datetime(df["Time"])
#df_time = df['Time'].dt.strftime("%H:%M:%S,%f")
df1 = df
#print(df1[df1["Time"]>=(dt.datetime.now()-dt.timedelta(hours=3))]["Operation"])
#print(df1)
last_hour_time = dt.datetime.now() - dt.timedelta(hours =3)
last_time = last_hour_time.strftime('%H:%M:%S,%f')
#print(last_hour_time)
#print(last_time[:-3])
last_nhour = last_time[:-3]
new_data = df[df["Time"] > last_nhour]
#print(new_data)
load_data = new_data[['user', 'Operation', 'Source', 'Destination']].sort_values(by='Destination', ascending=False)
print(build_table(load_data, 'blue_light'))
#print(load_data)
user_data = new_data[['user', 'Operation']]
##user_data1 = user_data.groupby(["user"]).sum().sort_values("Operation", ascending=False).head(1)
user_data1 = user_data[["user", "Operation"]].groupby(["user"]).count().sort_values("Operation", ascending=False)
print("#########################################")
print("User that had highest number of opertaion ")
print(user_data1)
#html_user_data = build_table(user_data1, 'blue_light')
#print(html_user_data)
print("##########################################")
plt.plot(user_data["user"], user_data["Operation"])
dir_data = new_data[['user', 'Operation', 'Destination']]
#new_dir = dir_data.groupby(["Destination"]).sum().sort_values("Operation", ascending=False)
new_dir = dir_data[["Destination", 'Operation']].groupby(["Destination"]).count().sort_values("Operation", ascending=False).head(3)
print("Directory having highest number of operations")
#dir_html = build_table(new_dir, 'blue_light')
print(new_dir)
#print(dir_html)
print("##########################################")
#print(dir_data["Operation"].count())
user_class_data = new_data[['user', 'Operation']]
print("Classification as per total number of operations")
print(user_class_data.groupby(["Operation"]).agg(["count"]))
print("##########################################")
