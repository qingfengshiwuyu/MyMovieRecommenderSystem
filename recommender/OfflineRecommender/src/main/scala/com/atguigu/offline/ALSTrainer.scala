package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {



  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //读取数据
    val ratingRDD = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGO_RATING_COLLECTIN)
      .load()
      .as[MovieRating]
      .rdd
      .map(x => Rating(x.uid,x.mid,x.score)).cache()
    //将数据分为两份，分别用在训练集和测试集
    // 将一个RDD随机切分成两个RDD，用以划分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))


    val trainRDD: RDD[Rating] = splits(0)
    val testRDD:RDD[Rating]= splits(1)

    //输出最优参数
    adjustALSParams(trainRDD, testRDD)

    spark.stop()
  }



  def adjustALSParams(trainRDD: RDD[Rating], testRDD: RDD[Rating]): Unit = {
    // 这里指定迭代次数为5，rank和lambda在几个值中选取调整
    val result = for(rank <- Array(100,200); lambda <- Array( 0.01, 0.001))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainRDD,rank,5,lambda)
        //计算均方根
        val rmse = getRMSE(model, testRDD)
        (rank,lambda,rmse)
      }
    // 按照rmse排序
    println("========================"+result.sortBy(_._3).head+"===================")

  }

  def getRMSE(model: MatrixFactorizationModel, testRDD: RDD[Rating]):Double = {
    val userMovie: RDD[Rating] = model.predict(testRDD.map(x=>(x.user,x.product)))

    //真实数据
    val real: RDD[((Int, Int), Double)] = testRDD.map(x =>((x.user,x.product),x.rating))
    val predict: RDD[((Int, Int), Double)] = userMovie.map(x =>((x.user,x.product),x.rating))

    //求得均方根
    Math.sqrt {
      real.join(predict).map{
        case ((uid,mid),(real,pre)) =>
          val err = real - pre
          err * err
      }.mean()
    }

  }
}
