package com.atguigu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.Implicits.tupleToGeoCoords
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//定义样例类，用来封装那些字段
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,
                 shoot:String,language:String,genres:String,actors:String,director:String)
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)

case class MongoConfig(uri:String,db:String)

case class Recommendation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {

  val  MONGO_MOVIE_COLLECTION = "Movie"
  val  MONGO_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIE = "RateMoreMovie"
  val RATE_MORE_RECENTLY_MOVIE = "RateMoreRecentlyMovie"
  val AVERAGE_MOVIE = "AverageMovie"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db"-> "recommender"
    )
    //创建sparksession
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._
    //读取数据
    val ratingDF: DataFrame = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",MONGO_MOVIE_COLLECTION)
      .load()
      .as[Movie]
      .toDF()

    
    //评分表注册为一张rating表
    ratingDF.createOrReplaceTempView("ratings")

    //1、历史热门电影统计,看电影评论次数多的
    val ratingMoreMovieDF: DataFrame = spark.sql("select mid,count(mid) as count from ratings group by mid order by count DESC ")
    storeDataInMongoDB(ratingMoreMovieDF,RATE_MORE_MOVIE)

    //2、最近热门电影统计
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个udf函数，用来转换时间
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x*1000l)).toInt)

    val ratingOfYearMonth: DataFrame = spark.sql("select mid,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyMovieDF: DataFrame = spark.sql("select mid,count(mid) as count,yearmonth  from ratingOfMonth group by yearmonth,mid order by yearmonth,count ")
    storeDataInMongoDB(rateMoreRecentlyMovieDF,RATE_MORE_RECENTLY_MOVIE)

    //3、电影平均得分统计
    val averageMovieDF: DataFrame = spark.sql("select mid,avg(score) as avg from ratings group by mid ")
    storeDataInMongoDB(averageMovieDF,AVERAGE_MOVIE)

    //4、每个类别优质电影统计
    //所有的电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
    //将所有的电影类别转换为RDD
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)

    //电影RF join 分数RF 让电影详细表带上分数
    val movieWithScoreDF: DataFrame = movieDF.join(averageMovieDF,Seq("mid","mid"))

    //电影类别做笛卡尔积
    val genresToMoviesDF: DataFrame = genresRDD.cartesian(movieWithScoreDF.rdd)
      .filter { //将那些带有本身genres的留下
        case (genres, row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }.map { //减少数据量
      case (genres, row) => (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
    }.groupByKey().map { //按avg分数排序，拿出前十个来
      case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1, x._2)))
    }.toDF()
    storeDataInMongoDB(genresToMoviesDF,GENRES_TOP_MOVIES)


    spark.stop()

  }

  def storeDataInMongoDB(df:DataFrame,collection:String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",collection)
      .mode("overwrite")
      .save()
  }


}
