import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q4\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['user', 'favouritescount'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = all_data['user']
print(labels)
sizes = all_data['favouritescount']
index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('username')
plt.ylabel('favourites Count')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('user with highest favourites count')

plt.show()

#bar_graph()

