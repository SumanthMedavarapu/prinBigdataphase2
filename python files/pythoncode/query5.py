import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q5\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['location', 'nooftweets'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = all_data['location']
print(labels)
sizes = all_data['nooftweets']
index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('location')
plt.ylabel('nooftweets')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('More tweets coming from location')

plt.show()

#bar_graph()

