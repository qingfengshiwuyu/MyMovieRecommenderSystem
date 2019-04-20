package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,
                 shoot:String,language:String,genres:String,actors:String,director:String)
case class MovieRating(uid:Int,mid:Int,score:Double,timestamp:Int)

case class MongoConfig(uri:String,db:String)

// 标准推荐对象，mid,score
case class Recommendation(mid: Int, score:Double)

// 用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 电影相似度（电影推荐）
case class MovieRecs(mid: Int, recs: Seq[Recommendation])



object OfflineRecommender {



  val MONGO_MOVIE_COLLECTION = "Movie"
  val MONGO_RATING_COLLECTIN ="Rating"
  //推荐表的名称
  val USER_RECS = "UserResc"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri"->"mongodb://hadoop102:27017/recommender",
      "mongo.db"->"recommender"
    )

    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._


    //读取数据，让其变为（uid,mid,score）形式
    val ratingRDD: RDD[(Int, Int, Double)] = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTIN)
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()
    //取到全部用户
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    //取到所有的电影
    val movieRDD: RDD[Int] = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .load()
      .as[Movie]
      .rdd
      .map(movie => movie.mid).cache()

    //将ratingRDD转化为RDD[Rating]类型的

    val trainRDD: RDD[Rating] = ratingRDD.map(x => Rating(x._1,x._2,x._3))

    //定义三个参数
    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参
    val (rank,iterations,lambda) = (50, 5, 0.01)
    //通过ALS算法计算模型
    val model: MatrixFactorizationModel = ALS.train(trainRDD,rank,iterations,lambda)


    //user和movie组成的RDD
    val userAndMovieRDD: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)
    //通过模型得到训练之后的结果
    val preRatings: RDD[Rating] = model.predict(userAndMovieRDD)


    //处理结果
    val userRescDF: DataFrame = preRatings.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()  //user聚合，取前十
      .map {
        case (user, resc) => UserRecs(user, resc.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    //结果写入到MongoDB中
    userRescDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .save()
    
    
    //计算电影相似度矩阵


    //转换为Java中能做矩阵运算的类
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (movie, features) => (movie, new DoubleMatrix(features))
    }
    //各个电影做笛卡尔积
    val movieFeaturesDF: DataFrame = movieFeatures.cartesian(movieFeatures)
      .filter {
        //取得两个电影不相似的
        case (a, b) => a._1 != b._1
      }.map {
      case (a, b) =>
        val simScore = this.consinSim(a._2, b._2) //定义一个方法，求余弦相似度
        (a._1, (b._1, simScore))  //(movie1,(movie2,相似度))
    }.filter(_._2._2 > 0.6)  //取相似度大于0.6的
      .groupByKey() //按照每一个movie聚合
      .map {
        case (movie, items) => MovieRecs(movie, items.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()

    //将电影相似度写入MongoDB中
    movieFeaturesDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .save()

    spark.stop()


  }
  def consinSim(x: DoubleMatrix, y: DoubleMatrix):Double = {
    return x.dot(y)/(x.norm2()*y.norm2())
  }
}
