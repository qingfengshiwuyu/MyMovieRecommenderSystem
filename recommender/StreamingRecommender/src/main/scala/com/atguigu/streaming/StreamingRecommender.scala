package com.atguigu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.convert.WrapAsJava.mapAsJavaMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//定义一个连接对象，包含了jedis和MongoDB的连接
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("hadoop102",6379)
  lazy val mongClint = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)

case class Recommender(mid:Int,score:Double)

case class UserRecs(uid:Int,resc:Seq[Recommender])

case class MovieRecs(mid:Int,recs:Seq[Recommender])

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamingRecs"
  val MONGODB_RATINGS_COLLECTION ="Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"




  def main(args: Array[String]): Unit = {
    val config = Map(

      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建各个对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val ssc = new StreamingContext(sc,Durations.seconds(2))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._


    //读取电影相似度矩阵
    val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        recs => (recs.mid, recs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()
    //将电影相似度矩阵广播出去
    val simMovieMatrixBroadcast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simMovieMatrix)

    //建立kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" ->"latest"
    )

    //创建kafkastreaming

    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")), kafkaPara))


    //参数评分流，格式为  UID|mid|score|timetemp
    val RatingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map { case msg =>
      val attr: Array[String] = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    //核心的算法
    RatingStream.foreachRDD{
      rdd => rdd.map{
        case (uid,mid,score,timeStamp) =>
          println("=============================")

        //1、从Redis中获取当前最近M次的评分(mid,score)
        val userRecentlyRating: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

        //2、获取与P相似的K个电影(排除掉已经看过的物品） Array(mid,mid)
        val simMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMovieMatrixBroadcast.value)


        //3、计算相似电影与最近几次评过分的电影优先级
        val streamRecs: Array[(Int, Double)] = computeMovieScores(simMovieMatrixBroadcast.value,userRecentlyRating,simMovies)

        //4、存入MongoDB中

        saveRecsToMongodb(uid,streamRecs)

      }.count()
    }


    ssc.start()

    print("ssc starting..........................")
    ssc.awaitTermination()
  }

  /**
    * 获取用户最近的几次评分
    * @param num 取多少个
    * @param uid 用户ID
    * @param jedis 从Redis中取
    * @return 放回的为（mid，score）
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int,Double)]= {

    import scala.collection.JavaConversions._
    //从jedis取出num个，将他们的值封装为（mid,score）
    jedis.lrange("uid:" + uid,0,num).map{
      items =>
        val attr: Array[String] = items.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取用户评价电影的相似度num个电影（剔除已经看过的）
    * @param num  取几个电影
    * @param mid  刚评价的电影
    * @param uid   用户ID
    * @param simMovies    电影相似度矩阵
    * @return
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {

    //从电影相似度矩阵中获取到评价电影相似的矩阵
    val allSimMovies: Array[(Int, Double)] = simMovies.get(mid).get.toArray

    //从MongoDB的Rating取出用户已经看过的电影
    val ratingExist: Array[Int] = ConnHelper.mongClint(mongoConfig.db)(MONGODB_RATINGS_COLLECTION)
      .find(MongoDBObject(("uid" -> uid))).toArray.map {
      item =>
        item.get("mid").toString.toInt
    }

    //相似度矩阵中，过滤掉已经看过的电影
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(_._1)
  }




  /**
    * 计算电影的优先级
    *
    * @param simMovie  电影相似度矩阵,用来匹配备选电影与近期电影的相似度
    * @param userRecentlyRatings  用户近期评估的电梯
    * @param topSimMovies  当前电影相似的n个电影
    * @return
    */
  def computeMovieScores(simMovie: collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]): Array[(Int,Double)]= {
    //定义一个arraybuffer保存得分(备选电影，和近期电影的得分)
    val  score = ArrayBuffer[(Int,Double)]()
    //定义一个hashmap用来保存近期评分大于3的个数（备选电影，大于3的个数）
    val increMap = mutable.HashMap[Int,Int]()
    //定义一个hashmap用来保存近期评分小于3的个数（备选电影，大于3的个数）
    val decreMap = mutable.HashMap[Int,Int]()


    for(topSimMovie <- topSimMovies;userRecentlyRating <- userRecentlyRatings){
      val simScore: Double = getMovieSimScore(simMovie,userRecentlyRating._1,topSimMovie)
      if(simScore > 0.6){
        //(备选电影，相似度*评分)
        println("simScore:" + simScore)
        score +=((topSimMovie,simScore * userRecentlyRating._2))
        //如果用户前几次评分大于3，则在hashmap中加1
        if(userRecentlyRating._2 >3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        } else{
          decreMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        }

      }
    }


    //分数按备选电影做聚合,聚合完（mid，（mid,sim））
    val result: Array[(Int, Double)] = score.groupBy(_._1).map {
      case (mid, sims) =>
        println("hahaha:" +  sims.map(_._2).sum)
        //按照那个公式去套
        (mid, (sims.map(_._2).sum / sims.length) + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray
    result
  }

  /**
    * 备选电影和近期评价电影的相似度
    * @param simMovie
    * @param userRecentlyMovie
    * @param topSimMovie
    * @return
    */
  def getMovieSimScore(simMovie: collection.Map[Int, Map[Int, Double]], userRecentlyMovie: Int, topSimMovie: Int):Double = {

    //从电影矩阵中，找备选电影的类表，在类表找是否有最近评价过的。有返回它的相似度
    simMovie.get(topSimMovie) match {
      case Some(sim) => sim.get(userRecentlyMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def log(i: Int):Double = {
    math.log(i) / math.log(10)
  }



  /**
    * 将推荐出来的结果存入到MongoDB中
    * @param uid
    * @param streamRecs
    * @param mongoConfig
    */
  def saveRecsToMongodb(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    //建立MongoDB连接
    val streamCollection: MongoCollection = ConnHelper.mongClint(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    //如果UID对应的东西的话就删除
    streamCollection.findAndRemove(MongoDBObject("uid"-> uid))
    streamCollection.insert(MongoDBObject("uid" -> uid,"recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1 ,"score" -> x._2))))
  }
}
