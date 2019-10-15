package org.apache.griffin.measure.spark

import org.apache.griffin.measure.step.builder.udf.GriffinUDFs.{indexOf, matches, regReplace}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

/**
 * zhoucg
 * 2019-10-15
 */
object SparkUDFTest {

  /**
   * SparkSql支持自定义的函数UDF
   * 在Spark中，也支持Hive中的自定义函数。自定义函数大致可以分为三种：
   *    UDF(User-Defined-Function)，即最基本的自定义函数，类似to_char,to_date等
   *    UDAF（User- Defined Aggregation Funcation），用户自定义聚合函数，类似在group by之后使用的sum,avg等
   *    UDTF(User-Defined Table-Generating Functions),用户自定义生成函数，有点像stream里面的flatMap
   *
   *    自定义一个UDF函数需要继承UserDefinedAggregateFunction类，并实现其中的8个方法
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 注册自定的UDF函数为临时函数
    val conf = new SparkConf().setAppName("SparkSession").setMaster("local")
    conf.set("spark.testing.memory", "2147480000")
    //val sc = new SparkContext(conf)
    //val context = new SQLContext(sc)

    // 适用sparksession进行创建对应的sparkContext和对应的sqlContext
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val sqlContext = sparkSession.sqlContext
    val sc = sparkSession.sparkContext

    val hiveContext = new HiveContext(sc)

    // 注册称为临时函数
    hiveContext.udf.register("get_distinct_city",GetDistinctCityUDF)

    // 注册称为临时函数
    hiveContext.udf.register("get_product_status",(str:String) =>{
      var status = 0
      for(s <- str.split(",")){
        if(s.contains("product_status")){
          status = s.split(":")(1).toInt
        }
      }
    })
  }
}
