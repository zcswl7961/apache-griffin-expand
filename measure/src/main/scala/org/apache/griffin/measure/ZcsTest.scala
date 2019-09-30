package org.apache.griffin.measure

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * scala语法
 * zhoucg
 * 2019-09-29
 */
object ZcsTest {

  def main(args: Array[String]): Unit = {
    var str:String = "zhoucg"
    println(str)
    var t1 = 3
    println(t1)
    val ch:Char = 'A'
    val toInt = ch.toInt
    println(toInt)
    val x =2
    val test = if(x>0) 1 else -1
    println(test)
    val x0 = 1
    val y0 = 1
    val x1 = 2
    val y1 = 2
    val distance = {
      val dx = x1 - x0
      val dy = y1 - y0
      Math.sqrt(dx*dx+dy*dy)
    }
    println(distance)

    // 循环语句设置
    val s = "hello"
    for (i <- 0 to s.length-1) {println(s(i))}
    for (i <-1 to 10) {println(i)}
    println("================================")
    // 跳出循环实例,break,
    breakable(
      for(i <-1 to 10){
        if(i == 5){
          break()
        }
      }
    )
    // 跳出循环实例，continue
    for(j<-1 to 10) {
      breakable{
        if(j == 5) {
          break
        }
        println(j)
      }
    }
    // 方法的定义设置数据
    // 定义格式   def 方法名 （参数列表）：返回值类型= {方法体}
    val addInt = add(1,2);
    println(addInt)
    // 多参数的设置
    val multAdd = addThenMultiply(2,3)(4)
    println(multAdd)

    println("========================================================")
    // 初始化一个长度为9的定长数组，器所有元素均为0
    val arr1 = new Array[Int](9)
    println(arr1)
    // 将数组转换成数组缓冲，就可以看到原数组中的内容了
    //println(arr1.toBuffer)//toBuffer会将数组转换长数组缓冲

    // 初始化一个长度为1值为9的定长数组
    val array2 = Array[Int](9)
    println(array2.toBuffer)

    // 使用（index）来访问元素
    println(array2(0))

    // 遍历数组
    for(x <-1 to(arr1.length - 1)) {
      println(arr1(x))
    }





    // 变长数组
    // 定义变长数组的方法
    var arr3 = new ArrayBuffer[Int](10)
    // 等价于
    var arr5 = new ArrayBuffer[Int]()
    // 使用+= 在尾端添加一个或者多个元素
    arr5 += 1
    arr5 += (2,3,4)
    //使用++=在尾端添加任何集合
    arr5 ++= Array(5,6,7)

    //使用append追加一个或者多个元素
    arr5.append(8)
    arr5.append(9,10)

    // 映射（Map）
    // 在Scala中，把哈希表这种数据结构叫做映射
    // 创建一个不可变的Map
    val scores =  Map("zhangsan"->90,"lisi"->80,"wangwu"->70)
    val scores_1 = Map(("zhangsan",90),("lisi",80),("wangwu",70))
    // 创建一个可变的Map
    val scores1 = scala.collection.mutable.Map(("zhangsan",90),("lisi",80),("wangwu",70))
    // 根据键获取map对应的值，可以有以下三种方法，尤其推荐使用getOrElse
    val score1 = scores("lisi")
    val score2 = if(scores.contains("lisi")) scores("lisi") else 0
    val score3 = scores.getOrElse("lisi",0)  //0为默认值，如果不存在则返回默认值


    // 元祖 （Tuple）
    // 集合高级运用
    val seq = Seq(1,2,3,4,5)
    val min = seq.min
    println(min)

    val max = seq.max
    //
    seq.count(_%2 == 0)
    // map : 列表元素
    // map函数的逻辑是遍历集合中的元素并对每个元素调用函数
    seq.map(n=>n*2)
    seq.map(_*2)

    // 当有一个集合的集合，然后你想对这些集合的所有元素进行操作时，就会用到 flatten。
    val s1 = Seq(1,2,3)
    val s2 = Seq(4,5,6)
    // flatMap数据操作
    val list = List(1,2,3,4,5)
    val finalList = list.flatMap(n=>List(n*10,n)) //map后压平
    println(finalList.toBuffer)


    val seqSingle:Seq[Int] = Seq(1,2,3,4,5,6)

    val names = List("About","Box","Clear")
    val namesMap = names.map{name => upper(name)}
    val flagNameMap = names.flatMap(name1 => upper(name1))
    println("namesMap"+namesMap)
    val flagNameMapFull = names.flatMap{name2 => upper(name2)}
    println("flagNameMapFull:"+flagNameMapFull)


  }

  def upper(str:String):String = {
    str.toLowerCase;
  }

  // 简单参数的add信息
  def add(x:Int,y:Int):Int = {
    x+y;
  }

  // 带有多参数的add数据的设置
  def addThenMultiply(x:Int,y:Int)(multiplier:Int):Int = {
    (x+y)* multiplier
  }

  // 无参方法
  def nameVoidStr:String = {
    System.getProperty("user.name")
  }

  def voidType:Unit = {
    println("这个是一个无参的函数信息");
  }


}
