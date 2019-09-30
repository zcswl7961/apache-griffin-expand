package org.apache.griffin.measure.spark

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * https://www.cnblogs.com/qingyunzong/p/8987579.html#_label0
 * SparkSQL学习
 * zhoucg
 * 2019-09-30
 */
object SparkSqlTest {

  /**
   * 认识spark sql：
   * spark SQL是spark的一个模块，主要用于进行结构化数据的处理。它提供的最核心的编程抽象就是DataFrame。
   * spark sql 提供一个编程抽象（DataFrame） 并且作为分布式 SQL 查询引擎
   * DataFrame：它可以根据很多源进行构建，包括：结构化的数据文件，hive中的表，外部的关系型数据库，以及RDD
   * 运行原理：
   * 将spark sql 转换为RDD，然后提交到集群执行
   *
   * SparkSession是Spark 2.0引如的新概念。SparkSession为用户提供了统一的切入点，来让用户学习spark的各项功能。
   * 在spark的早期版本中，SparkContext是spark的主要切入点，
   * 由于RDD是主要的API，我们通过sparkcontext来创建和操作RDD。对于每个其他的API，我们需要使用不同的context。
   *
   * SparkSession实际上就是SqlContext和HiveContext的组合，（未来可能还会加上StreamingContext），
   * 所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
   * SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSession").setMaster("local")
    conf.set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
//    joinDataFram(sc,context)
    mysqldataFrameRead(sc,context)


  }

  def caseClass(sc:SparkContext,context:SQLContext) : Unit = {
    // 将本地的数据读入 RDD， 并将 RDD 与 case class 关联
    val peopleRDD = sc.textFile("F:\\people.txt")
      .map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))

    import context.implicits._
    // 将RDD 转换成 DataFrames
    val df = peopleRDD.toDF
    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    context.sql("select * from people").show()
  }

  /**
   * 方式二：通过 structType 创建 DataFrames（编程接口）
   * @param sc
   * @param context
   */
  def structType(sc:SparkContext,context:SQLContext) : Unit = {
    val fileRDD = sc.textFile("F:\\people.txt")
    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row
    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })

    // 创建structType 来定义结构
    val structType: StructType = StructType(
      //字段名，字段类型，是否可以为空
      StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil
    )
    /**
     * rows: java.util.List[Row],
     * schema: StructType
     * */
    val df:DataFrame = context.createDataFrame(rowRDD,structType)
    df.createOrReplaceTempView("people")
    context.sql("select * from people").show()

  }

  /**
   * 方式三，通过json文件构建DataFrame
   * @param sc
   * @param context
   */
  def joinDataFram(sc:SparkContext,context:SQLContext) : Unit = {
    val df: DataFrame = context.read.text("F:\\people.txt")
    df.createOrReplaceTempView("people")
    context.sql("select * from people").show()
  }


  /**
   * DataFrame的read和save和savemode
   * 数据的读取
   * @param sc
   * @param context
   */
  def dataFrameRead(sc:SparkContext,context:SQLContext) : Unit = {
    //方式一
    val df1 = context.read.json("E:\\666\\people.json")
    val df2 = context.read.parquet("E:\\666\\users.parquet")
    //方式二
    val df3 = context.read.format("json").load("E:\\666\\people.json")
    val df4 = context.read.format("parquet").load("E:\\666\\users.parquet")
    //方式三，默认是parquet格式
    val df5 = context.load("E:\\666\\users.parquet")
  }


  /**
   * 数据源之mysql
   * @param sc
   * @param context
   */
  def mysqldataFrameRead(sc:SparkContext,context:SQLContext): Unit = {

    val url = "jdbc:mysql://192.168.123.102:3306/test"
    val table = "users"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")

    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val df =  context.read.option("driver","com.mysql.jdbc.Driver").jdbc(url,table,properties)
    df.createOrReplaceTempView("dbs")
    context.sql("select * from dbs").show()


  }

  case class People(name:String,age:Int) {

    val localName = name
    val localAge = age
  }
}
