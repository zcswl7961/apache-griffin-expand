package org.apache.griffin.measure.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark分区原理及方法
 * zhoucg
 * 2019-09-30
 */
object SparkPartitionTest {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("ApplicationTestSpark").setMaster("local")
    /**
     * local[n]：n是几表示的就是几分分区
     * 如果n为* 表示就等于cpu core的个数
     */
    val conf = new SparkConf().setAppName("ApplicationTestSpark").setMaster("local[*]")
    conf.set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    //默认只有一个分区
    val list = List(1,2,3,4)
    val listRDD:RDD[Int] = sc.parallelize(list)
    val numPartitions = listRDD.getNumPartitions
    println(numPartitions)
  }
}
