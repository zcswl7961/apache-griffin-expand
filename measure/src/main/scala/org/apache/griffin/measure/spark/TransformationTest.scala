package org.apache.griffin.measure.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 参考：https://www.cnblogs.com/qingyunzong/p/8922135.html
 * Spark Transformation和Action
 * zhoucg
 * 2019-09-30
 */
object TransformationTest {

  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("TransformationTestApp").setMaster("local")
    conf.set("spark.testing.memory", "2147480000")
    val sparkContext:SparkContext = new SparkContext(conf)
    //map(sparkContext)
    //flatMap(sparkContext)
    //mapParations(sparkContext)
    //reduce(sparkContext)
    reduceByKey(sparkContext)
  }

  /**
   * map：
   * 它是将源RDD的一个一个元素传入call方法，并经过算法后一个一个的返回从而生成新的RDD
   * 总结：
   * 可以看出，对于map算子，源JavaRDD的每个元素都会进行计算，由于是依次进行传参，
   * 所以他是有序的，新RDD的元素顺序与源RDD是相同的。而由有序又引出接下来的flatMap。
   */
  def map(sc:SparkContext):Unit = {
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    println(listRDD)
    val nameRDD = listRDD.map(name => "Hello" +name)
    nameRDD.foreach(name => println(name))
  }

  /**
   * flagMap与Map一样，是将RDD中的元素依次传入call方法，他比map多的功能是能在任何一个传入call方法的元素后面添加任意多元素，
   * 而能达到这一点，正是因为其进行传参是依次进行的。
   * flatMap的特征决定了这个算子在对需要随时增加元素的时候十分好用，比如对源RDD查漏补缺时
   * map和flatMap都是依次进行参数传递的
   * 但有时候需要RDD中的两个元素进行相应操作时（例如：算存款所得时，下一个月所得的利息是要原本金加上上一个月所得的本金的），
   * 这两个算子便无法达到目的了，这是便需要mapPartitions算子，
   * 他传参的方式是将整个RDD传入，然后将一个迭代器传出生成一个新的RDD，由于整个RDD都传入了，所以便能完成前面说的业务。
   * @param sc
   */
  def flatMap(sc:SparkContext) :Unit = {
    val list = List("张无忌 赵敏","宋青书 周芷若")
    val listRDD = sc.parallelize(list)

    val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello" +name)
    nameRDD.foreach(name => println(name))

  }

  /**
   * mapPartitions
   * @param sc
   */
  def mapParations(sc:SparkContext):Unit = {
    val list = List(1,2,3,4,5,6)
    val listRDD = sc.parallelize(list,2)
    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext){
        newList.append("hello " + iterator.next())
      }
      newList.toIterator
    }).foreach(name => println(name))
  }

  /**
   * 每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥？
   * @param sc
   */
  def mapPartitionsWithIndex(sc:SparkContext) : Unit = {
    val list = List(1,2,3,4,5,6,7,8)
    sc.parallelize(list).mapPartitionsWithIndex((index,iterator) => {
      val listBuffer:ListBuffer[String] = new ListBuffer
      while (iterator.hasNext){
        listBuffer.append(index+"_"+iterator.next())
      }
      listBuffer.iterator
    },true)
      .foreach(println(_))
  }

  /**
   * reduce其实是将RDD中的所有元素进行合并，当运行call方法时，会传入两个参数，
   * 在call方法中将两个参数合并后返回，而这个返回值回合一个新的RDD中的元素再次传入call方法中，
   * 继续合并，直到合并到只剩下一个元素时。
   * @param sc
   */
  def reduce(sc:SparkContext):Unit = {

    val list = List(1,2,3,4,5,6)
    val listRDD = sc.parallelize(list)

    val result = listRDD.reduce((x,y) => x+y)
    println(result)
  }

  /**
   * reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并。
   * @param sc
   */
  def reduceByKey(sc:SparkContext): Unit = {
    val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
    val mapRDD = sc.parallelize(list)

    val resultRDD = mapRDD.reduceByKey(_+_)
    resultRDD.foreach(tuple => println("门派: " + tuple._1 + "->" + tuple._2))
  }

  /**
   * 当要将两个RDD合并时，便要用到union和join，其中union只是简单的将两个RDD累加起来，
   * 可以看做List的addAll方法。就想List中一样，当使用union及join时，必须保证两个RDD的泛型是一致的。
   * @param sc
   */
  def union(sc:SparkContext):Unit ={

    val list1 = List(1,2,3,4)
    val list2 = List(5,6,7,8)

    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

    list1RDD.union(list2RDD).foreach(println(_))
  }

  /**
   * union只是将两个RDD简单的累加在一起，而join则不一样，join类似于hadoop中的combin操作，只是少了排序这一段，
   * 再说join之前说说groupByKey，因为join可以理解为union与groupByKey的结合：
   * groupBy是将RDD中的元素进行分组，组名是call方法中的返回值，而顾名思义groupByKey是将PairRDD中拥有相同key值得元素归为一组。即：
   * @param sc
   */
  def groupByKey(sc:SparkContext) :Unit = {
    val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
    val listRDD = sc.parallelize(list)
    val groupByKeyRDD = listRDD.groupByKey()
    groupByKeyRDD.foreach(t => {
      val menpai = t._1
      val iterator = t._2.iterator
      var people = ""
      while (iterator.hasNext) people = people + iterator.next + " "
      println("门派:" + menpai + "人员:" + people)
    })
  }

}
