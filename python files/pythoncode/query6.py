import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q6\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['screenname', 'favouritescount'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = all_data['screenname']
print(labels)
sizes = all_data['favouritescount']
index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('screenname')
plt.ylabel('favouritescount')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('tweets having high num of likes')

plt.show()

#bar_graph()

