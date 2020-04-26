import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:\\spark-2.4.5-bin-hadoop2.7\\")


    val employeeDF=spark.read.json("F:\\PBPhase2\\tweetsdata.txt")
    //query1
    employeeDF.createOrReplaceTempView("parquetFile")
    val place=spark.sql("SELECT distinct place.country, count(*) as count FROM parquetFile where place.country is not null " + "GROUP BY place.country ORDER BY count DESC LIMIT 10")
    //namesDF.map(attributes => "Name:" + attributes(0)).show()
    //place.show()
    place.coalesce(1).write.csv("F:\\PBPhase2\\q1")

    //query2 which location is having max followers
    val location=spark.sql("select distinct user.location, max(user.followers_count) AS followers_count from parquetFile where user.location is not null group by user.location order by followers_count  desc LIMIT 10")
    //location.show()
    location.coalesce(1).write.csv("F:\\PBPhase2\\q2")

    //query3  user having highest no of friends
    val highest_friends=spark.sql("select distinct user.name,user.friends_count from parquetFile order by friends_count desc limit 10")
    //highest_friends.show()
    highest_friends.coalesce(1).write.csv("F:\\PBPhase2\\q3")

    //query 4  Top 10 Users Who are actively liking tweets.
    val active_liking= spark.sql("select user.name,user.favourites_count from parquetFile order by user.favourites_count desc limit 15")
    //active_liking.show()
    active_liking.coalesce(1).write.csv("F:\\PBPhase2\\q4")
    //query 5 More tweets coming from locations
    val more_tweets= spark.sql("select user.location as location,count(*) as nooftweets from parquetFile where user.location is not null group by user.location order by count(1) desc limit 10")
    //more_tweets.show()
    more_tweets.coalesce(1).write.csv("F:\\PBPhase2\\q5")
    //query 6 tweets having high num of likes
    val high_likes= spark.sql("select user.screen_name,user.favourites_count from parquetFile order by favourites_count desc LIMIT 10")
    high_likes.coalesce(1).write.csv("F:\\PBPhase2\\q6")
    //query 7 Account verification
    val Accountverification=spark.sql("SELECT distinct id,CASE WHEN user.verified like '%true%' THEN 'VERIFIED ACCOUNT' WHEN user.verified like '%false%' THEN 'NOT VERIFIED ACCOUNT' END AS Verify FROM parquetFile")
    Accountverification.createOrReplaceTempView("acctVerify")
    val verifieddata=spark.sql("SELECT Verify,count(Verify) as count from acctVerify where id is NOT NULL group by Verify order by count DESC")
    //verifieddata.show()
    verifieddata.coalesce(1).write.csv("F:\\PBPhase2\\q7")


    //query 8 users created in a month
    val users_created=spark.sql("SELECT substring(created_at, 1,10) as month, count(1) users from parquetFile group by month")
    //users_created.show()
    users_created.coalesce(1).write.csv("F:\\PBPhase2\\q8")

    //query 9 corona tweets
    val coronatext=spark.sql("SELECT count(text) as count from parquetFile where text like '%corona%'")
    val extendedtweet=spark.sql("SELECT count(extended_tweet.full_text) as count from parquetFile where extended_tweet.full_text like '%corona%'")
    val corona_retweetedstatus=spark.sql("SELECT count(retweeted_status.text ) as count from parquetFile where retweeted_status.text like '%corona%'")
    val totalcoronatweets=coronatext.union(extendedtweet).union(corona_retweetedstatus)
    //totalcoronatweets.show()
    totalcoronatweets.coalesce(1).write.csv("F:\\PBPhase2\\q9")

    //query 10 is user protected or not
    //val user_protected= spark.sql("select user.protected from parquetFile where user.protected like '%true%'")
    //user_protected.coalesce(1).write.csv("F:\\PBPhase2\\q10")
    //query10 TOTAL TWEETS success
    val  text=spark.sql("SELECT  count(text) as count from parquetFile")
    val extended_tweet=spark.sql("SELECT count(extended_tweet.full_text) as count from parquetFile")
    val retweeted_status=spark.sql("SELECT count(retweeted_status.text ) as count from parquetFile")
    val total_tweets=text.union(extended_tweet).union(retweeted_status)
    //total_tweets.show()
    total_tweets.coalesce(1).write.csv("F:\\PBPhase2\\q10")




  }

}
