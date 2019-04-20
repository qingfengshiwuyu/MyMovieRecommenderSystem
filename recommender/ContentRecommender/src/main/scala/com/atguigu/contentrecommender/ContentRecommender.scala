package com.atguigu.contentrecommender

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


// 创建样例类
// 基于LFM的CF推荐只需要movie和rating数据
case class Movie(mid:Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String
                )

// 创建mongodb配置样例类
case class MongoConfig(uri:String, db:String)

// 定义一个标准推荐对象
case class Recommendation(mid: Int, score: Double)


// 定义一个电影相似度列表对象
case class MovieRecs(mid: Int, recs: Seq[Recommendation])


object ContentRecommender {

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"

  // 最终结果存入mongodb的表名
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores"->"local[*]",
      "mongo.uri"->"mongodb://hadoop:27017/recommender",
      "mongo.db" -> "recommender"
    )


    val sparkConf: SparkConf = new SparkConf().setAppName("ContentRecommender").setMaster(config("spark.cores"))
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //读取文件，并做转换
    val movieTagsDF: DataFrame = spark.read
      .format("com.mongodb.spark.sql")
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .load()
      .as[Movie]
      .rdd
      .map(x => (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c)))
      .toDF("mid", "name", "genres").cache()


    //定义一个分词器，默认分隔符为空格
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    //用上面的分词器做转换(mid,name,genres,words)  words:[xxx,yyy,bbb,ddd]
    val wordsData: DataFrame = tokenizer.transform(movieTagsDF)

    //定义IF工具，这里使用hashingIF ,用来统计词频
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(25)

    //用上面的TF工具处理分词后的数据
    val featuredData: DataFrame = hashingTF.transform(wordsData)

    //定义IDF工具，用来计算文档的idf
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    //利用idf来训练模型
    val idfMondel: IDFModel = idf.fit(featuredData)

    //用训练的模型得到新的特征矩阵
    val rescaledData: DataFrame = idfMondel.transform(featuredData)

    //提取特征向量
    val movieFeatures: RDD[(Int, DoubleMatrix)] = rescaledData.map {
      case row =>
        (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    }.rdd
      .map(x => (x._1, new DoubleMatrix(x._2)))



    //跟隐语义模型得到特征向量相同的做法了
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
      .option("collection",CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .save()

    spark.stop()


  }
  def consinSim(x: DoubleMatrix, y: DoubleMatrix):Double = {
    return x.dot(y)/(x.norm2()*y.norm2())
  }

}
