
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q9\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['noofcoronatweets'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = ['coronatext', 'coronaExtendedtweets','coronaretweeted_status']
print(labels)
sizes = all_data['noofcoronatweets']









# Data to plot
#labels = ['Non-verified-accounts', 'Verified-Accounts']
#sizes = [204260, 1008]
colors = ['yellow', 'red','blue']

# Plot
plt.pie(sizes, labels=labels, colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
plt.show()
