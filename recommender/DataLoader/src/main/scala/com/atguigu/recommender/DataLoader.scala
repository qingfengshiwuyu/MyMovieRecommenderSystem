package com.atguigu.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.util.matching.Regex


//定义样例类，用来封装那些字段
case class Movie(mid:Int,name:String,descri:String,timelong:String,issue:String,
                 shoot:String,language:String,genres:String,actors:String,director:String)
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)
case class Tag(uid:Int,mid:Int,tag:String,timestamp:Int)


case class MongoConfig(uri:String,db:String)
case class ESConfig(httpHosts:String,transportHosts:String,index:String,
                    clustername:String)


object DataLoader {
  //定义一些常数
  val MOVIE_DATA_PATH="G:\\ideaCode\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH="G:\\ideaCode\\MovieRecommenderSystem\\recommender\\DataLoader" +
    "\\src\\main\\resources\\ratings.csv"
  val TAGS_DATA_PATH="G:\\ideaCode\\MovieRecommenderSystem\\recommender\\DataLoader" + "\\src" +
    "\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION ="Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION ="Tag"

  val ES_MOVIE_TYPE = "Movie"




  def main(args: Array[String]): Unit = {
    //定义一些配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db"-> "recommender",
      "es.httpHosts" -> "hadoop102:9200",
      "es.transportHosts" -> "hadoop102:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "my-application"
    )
    //创建sparksession对象
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("dataloader")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //创建movieDF
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF: DataFrame = movieRDD.map(items => {
      val attr: Array[String] = items.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5)
        .trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    //创建ratingDF
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(items=>{
      val attr: Array[String] = items.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    //创建tagsDF

    val tagRDD = spark.sparkContext.textFile(TAGS_DATA_PATH)
    val tagDF = tagRDD.map(items=>{
      val attr: Array[String] = items.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()




    //写入到mongodb中
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    storeDataInMongoDB(movieDF,ratingDF,tagDF)

    //处理数据

    import org.apache.spark.sql.functions._
    val newTag =  tagDF.groupBy($"mid")
        .agg(concat_ws("|",collect_set($"tag")).as("tags"))
        .select("mid","tags")

    val movieWithTagDF = movieDF.join(newTag,Seq("mid","mid"),"left")

    //写入到es中
    implicit val esConfig =  ESConfig(config("es.httpHosts"),config("es.transportHosts"),config("es.index"),config("es.cluster.name"))
    saveDataInES(movieWithTagDF)

    //关流

    spark.stop()


  }


  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)
                        (implicit mongoConfig: MongoConfig)
  = {
    //获取MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果有那个集合（表）就删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //数据存入MongoDB
    movieDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .save()

    ratingDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .save()

    tagDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .save()

    //对表创建索引

    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject
    ("mid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject
    ("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject
    ("mid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"
   ->1 ))

    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"
      ->1 ))

    mongoClient.close()

  }



  def saveDataInES(movieWithTagDF: DataFrame)(implicit esConfig:ESConfig) = {

    //创建es客户端
    val settings: Settings = Settings.builder().put("cluster.name",esConfig.clustername).build()
    val esClient = new PreBuiltTransportClient(settings)


    val REGEX_HOST_PORT: Regex = "(.+):(\\d+)".r
    //添加建立的连接  hadoop102:9200
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) =>{
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    //如果有这个index，则删除
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    //创建索引
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))


    movieWithTagDF.write
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes",esConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .save(esConfig.index + "/" + ES_MOVIE_TYPE)
//    esClient.close()


  }




}
