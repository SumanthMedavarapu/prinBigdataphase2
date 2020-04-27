# prinBigdataphase2
SOFTWARES USED

•	We used intellij IDEA for running spark sql queries, and written in scala language

•	 For visualization, we used matplotlib library in python ,which contains various graphs like bar graph, pie chart etc

•	Tkinter module used for Graphical user interface 

IMPLEMENTATION

•	we wrote a Python  program to stream the tweets and save them into JSON format, the output of the program contains the tweets with all the details like the IDs, Hashtags, Text, Users’s information, etc. 

•	The extracted JSON tweets are persisted into the Apache SparkSQL in the form of tables. We used 1 table to manage the data:
o	Table “parquetFile” to store each information of Json file .

•	We used Scala to write queries and saved the output in .csv file 

•	We used python to read the saved .csv file using glob module, and while reading csv file ,we need to give the column names and store in the form the dataframe using pandas module

•	After that we used matplotlib to visualize the data in pie chart, bar chart etc 

•	We used tkinter module for GUI programming to keep buttons and when we click on it, it will execute that particular query and output chart will display. here we have integrated everything using tkinter ,and written again everything python using pyspark.so if we click button ,it will run spark sql query and visualize the chart


1.	Load data

•	We need to read the generated json tweets data, after running the tweetsimport.py file 

•	In spark, it will store in the form of named column format ,and we give a table name

Steps to run the project

1)download PBproject folder and run the scala file,then it will run all the queries and save each query in separate .csv file(we can see in python files\outputcsvfiles)

2)next run the python files(python file\python code) for individual query and it will read data from csv file and it will plot in the chart(outputvisualization\visualization)


3)we integrated above 2 steps using pyspark module in python ,and created GUI
If user click on query buttons,it will execute that particular query , and display chart
For that run tkinter.py file present in (GUI programming\tkinter.py), and queries written in queries1.py ,and imported that file in tkinter.py
