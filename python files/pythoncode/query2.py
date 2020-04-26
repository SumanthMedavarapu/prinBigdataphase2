import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
#import openpyxl

#import json

#f = open("querydata.csv", "r")
#print(f.readline())

#for x in f:
 # print(x)
  
f = glob.glob("q2\*.csv")
#print(f)
#for f in glob.glob("*.csv"):
#    print(f)
df = pd.read_csv(f[0],delimiter=',',names=['location', 'MaxfollowersCount'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = all_data['location']
print(labels)

sizes = all_data['MaxfollowersCount']

index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('locationName')
plt.ylabel('MaxfollowersCount')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('Location and its followers count')

plt.show()

#bar_graph()

