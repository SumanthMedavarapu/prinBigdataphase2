from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import types
from datetime import date, timedelta, datetime
# from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import time
import gmplot
import subprocess


def convert_to_dict(output, indexName, valueName):
    genderDf = output.toPandas()
    result = genderDf.set_index(indexName).to_dict()
    return result.get(valueName)

# def barchart(inputdict, title, xlabel, ylabel):
#     fig = plt.figure(figsize=(6, 4))
#     fig.suptitle(title, fontsize=16)
#     ax = fig.add_axes([0.1, 0.2, 0.75, 0.5])
#     ax.set_xlabel(xlabel)
#     ax.set_ylabel(ylabel)
#     labels = inputdict.keys()
#     values = inputdict.values()
#     ax.bar(labels, values, width=0.5)
#     plt.show()

def piechart(diction):
    label = diction.keys()
    tweet_count = diction.values()
    colors = ['#ff9999', '#66b3ff']
    fig1, ax1 = plt.subplots()
    ax1.pie(tweet_count, colors=colors, labels=label,
            autopct='%1.1f%%', startangle=90)
    # draw circle
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    # Equal aspect ratio ensures that pie is drawn as a circle
    ax1.axis('equal')
    plt.tight_layout()
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query5.png')
# -------------------query 1:From which country, more tweets received for corona virus----------------
def query1():
    print("#")
    country_count = sc.sql("SELECT distinct place.country as Country, count(*) as count FROM parquetFile where place.country is not null " + "GROUP BY place.country ORDER BY count DESC")
    print(country_count)
    country_count.show(10)
    pandasconversion = country_count.toPandas()#["Country"].values.tolist()[:15]
    x=pandasconversion['Country']
    print(x)
    y=pandasconversion['count']
    print(y)
    #y = country_count.toPandas()["count"].values.tolist()[:15]
   # df = pd.read_csv(f[0],delimiter=',',names=['Country', 'Count'])
    #all_data = pd.DataFrame()
    #all_data = all_data.append(df,ignore_index=True)
    #print(type(all_data))

    #labels = all_data['Country']
    #print(labels)
    
    #sizes = all_data['Count']
    index = np.arange(len(x))
    print(index)
#which defines the length of total plot
    plt.figure(figsize=(20, 3))
#which defines width of bar
    plt.bar(x, y, width=0.3)
    plt.xlabel('Name')
    plt.ylabel('Count')
#which defines font size of xticks
    plt.xticks(index, x,fontsize=7)
    plt.title('countries and its count')

    #plt.show()

    #figure = plt.figure(figsize=(10, 10))
    """axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'green')
    plt.title("tweets distribution countries")
    plt.ylabel("Country code")
    plt.xlabel("Number of tweets")"""
    plt.savefig('F:\\query1.png')
    # result = convert_to_dict(country_count, 'Country', 'count')
    # barchart(result, 'count of tweets from each country about coronavirus','Country', 'No.of tweets')

# ------------------------------query 2: location wise tweets distribution on coronavirus in US-------------------------------------
def query2() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")
    #state_distribution = sc.sql("SELECT location, COUNT(location) AS count FROM totaltweetsdata WHERE Country='US' AND location is NOT NULL GROUP BY location ORDER BY count DESC")
    #state_distribution.show() 
    location=sc.sql("select distinct user.location as location, max(user.followers_count) as MaxfollowersCount from parquetFile group by user.location order by MaxfollowersCount  desc LIMIT 10")
    location.show()
    pandas = location.toPandas()#["location"].values.tolist()[:12]
    #sizes = state_distribution.toPandas()["count"].values.tolist()[:12]
    #explode = (0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 )  # only "explode" the 1st slice
    labels = pandas['location']
    print(labels)

    sizes = pandas['MaxfollowersCount']

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


    plt.savefig('F:\\query2.png')
    # result = convert_to_dict(state_distribution, 'location', 'count')
    # piechart(result)

# ---------------------------------query 3:Popular language used for tweets --------------------------------
def query3() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    #lang_data = sc.sql("SELECT lang, COUNT(*) AS Count FROM datatable WHERE lang IS NOT NULL GROUP BY lang ORDER BY Count DESC")
    highest_friends=sc.sql("select distinct user.name as name,user.friends_count as friendscount from parquetFile order by friendscount desc limit 10")
    #lang_data.show()
    all_data=highest_friends.toPandas()
    labels = all_data['name']
    print(labels)
    sizes = all_data['friendscount']
    index = np.arange(len(labels))
    print(index)
#which defines the length of total plot
    plt.figure(figsize=(20, 3))
#which defines width of bar
    plt.bar(index, sizes, width=0.3)
    plt.xlabel('username')
    plt.ylabel('friends Count')
#which defines font size of xticks
    plt.xticks(index, labels,fontsize=7)
    plt.title('user having highest no of friends')

    plt.savefig('F:\\query3.png')

    # res = convert_to_dict(lang_data, 'lang', 'Count')
    # piechart(res)

# -------------------------query 4: Time analysis of the tweets on coronavirus------------------------
def query4() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    
    active_liking= sc.sql("select user.name  username , favorite_count  favouritescount from parquetFile order by favorite_count  desc limit 15")
    active =active_liking.toPandas()
    labels = active['username']
    print(labels)
    sizes = active['favouritescount']
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
    plt.savefig('F:\\query4.png')
# -------------------------------query 5: verified accounts tweets about coronavirus--------------------------
def query5() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    more_tweets= sc.sql("select user.location as location,count(*) as nooftweets from parquetFile where user.location is not null group by user.location order by count(1) desc limit 10")
    more =more_tweets.toPandas()
    labels = more['location']
    print(labels)
    sizes = more['nooftweets']
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
    plt.savefig('F:\\query5.png')
# -----------------------query 6: top 10 retweets of users---------------
def query6() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    high_likes= sc.sql("select user.screen_name screenname ,user.favourites_count favouritescount from parquetFile order by favouritescount desc LIMIT 10")

    #user_retweet_count = sc.sql("SELECT user.screen_name as UserName, retweeted_status.retweet_count as count from datatable where user.name is not null order by count desc")
    #user_retweet_count.show(10) favourites_count
    x = high_likes.toPandas()
    #y = user_retweet_count.toPandas()
    labels = x['screenname']
    print(labels)
    sizes = x['favouritescount']
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
    plt.savefig('F:\\query6.png')

    # plt.show()
    #plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query6.png')
    # results = convert_to_dict(user_retweet_count, 'Retweet', 'Count')
    # barchart(results, 'Top 10 users with retweets on corona','Users', 'No.of tweets')
# #---------------------------query 7: Top 10 people who tweeted most about corona------------------------------
# def query7() :
    
#     first = devices_data.toPandas()
#     #last = devices_data.toPandas()["source"].str.index("</a>")
#     #text = devices_data.toPandas()["source"].values.tolist()
#     x =[]
#     for i in range(len(text)):
#         x.append(text[i][first[i]:last[i]])
#     y = devices_data.toPandas()["total_count"].values.tolist()[:10]


#     figure = plt.figure()
#     axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
#     plt.barh(x,y, color = 'blue')
#     # plt.title("Top ", len(x), " Devices")
#     plt.ylabel("Device name")
#     plt.xlabel("Number of Devices")
#     plt.title("Top Devices Used in the Tweets")
#     plt.show()
#     # device_distribution = convert_to_dict(devices_data,'source','total_count')
#     # barchart(device_distribution,'Top 10 devices used', 'Device Name', 'Tweet count')
def query7():
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    Accountverification=sc.sql("SELECT distinct id,CASE WHEN user.verified like '%true%' THEN 'VERIFIED ACCOUNT' WHEN user.verified like '%false%' THEN 'NOT VERIFIED ACCOUNT' END AS Verify FROM parquetFile")
    Accountverification.registerTempTable("acctVerify")
    verifieddata=sc.sql("SELECT Verify,count(Verify) count from acctVerify where id is NOT NULL group by Verify order by count DESC")
    y = verifieddata.toPandas()
    labels = y['Verify']
    print(labels)
    sizes = y['count']
    colors = ['yellow', 'red']

# Plot
    plt.pie(sizes, labels=labels, colors=colors,
    autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()

    plt.savefig('F:\\query7.png')

#--------------------------query 8: Top 10 of hashtags used -------------------------------
def query8() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    users_created=sc.sql("SELECT substring(created_at, 1,10) month, count(1) users from parquetFile group by substring(created_at, 1,10)")
    users = users_created.toPandas()
    #hashtagsDF.show(10)
    labels = users_created.toPandas()
    labels = users['month']
    print(labels)
    sizes = users['users']
    colors = ['yellow', 'red']

# Plot
    plt.pie(sizes, labels=labels, colors=colors,
    autopct='%1.1f%%', shadow=True, startangle=140)

    plt.axis('equal')
    plt.show()
    plt.savefig('F:\\query8.png')

# ------------------------query 9: analysis on user creation for past 10 years----------------------------
def query9() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    coronatext=sc.sql("SELECT count(text) AS count from parquetFile where text like '%corona%'")
    extendedtweet=sc.sql("SELECT count(extended_tweet.full_text) AS count from parquetFile where extended_tweet.full_text like '%corona%'")
    corona_retweetedstatus=sc.sql("SELECT count(retweeted_status.text ) AS count from parquetFile where retweeted_status.text like '%corona%'")
    totalcoronatweets=coronatext.union(extendedtweet).union(corona_retweetedstatus)
    total = totalcoronatweets.toPandas()
    labels = ['coronatext', 'coronaExtendedtweets','coronaretweeted_status']
    print(labels)
    sizes = total['count']
    colors = ['yellow', 'red','blue']
    plt.pie(sizes, labels=labels, colors=colors,
    autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()
    # plt.show()
    plt.savefig('F:\\query9.png')


#-------------------------query 10: Top 10 users with friends----------------------------------
def query10() :
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")

    text=sc.sql("SELECT  count(text) AS count from parquetFile")
    extended_tweet=sc.sql("SELECT count(extended_tweet.full_text) AS count from parquetFile")
    retweeted_status=sc.sql("SELECT count(retweeted_status.text ) AS count from parquetFile")
    total_tweets=text.union(extended_tweet).union(retweeted_status)
    
    
    
    data = total_tweets.toPandas()
    
    labels = ['text', 'Extended_tweet','retweeted_status']
    print(labels)
    sizes = data['count']
    colors = ['yellow', 'red','blue']

    plt.pie(sizes, labels=labels, colors=colors,
    autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()
    plt.savefig('F:\\query10.png')

def main():
    global sc
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    print("Spark session start")
    total_data = sc.read.json('F:\\PBPhase2\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")
    print("table loaded")
    #total_tweets_data = sc.sql("SELECT user.name as UserName,user.location as location,text,created_at,user.verified as userVerified,retweet_count,place.country_code as Country,user.location as state,extended_tweet.entities.hashtags.text AS Hashtags,coordinates.coordinates as coordinates from datatable where place.country_code is not null AND (text like '%Corona%' OR text like '%corona%' OR text like '%coronavirus%' OR text like '%Coronavirus%')")
    #total_tweets_data.registerTempTable("totaltweetsdata")
    print("tweets filtered")
    query1()
    
    #query2()
    # # query3()
    # # query4()
    # # query5()
    # query6()
    # # query7()
    # # query8()
    # # query9()
    # # query10()

    sc.stop()
    print("PySpark completed")
