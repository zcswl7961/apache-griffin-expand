package org.apache.griffin.measure.spark

/**
 * zhoucg
 * 2019-10-10
 */
class Person(val name:String,val age : Int) {

  println("Hello Spark")

  val x = 1
  if(x>1){
    println("6666")
  } else if(x<1) {
    println("哈哈")
  } else {
    println("呵呵")
  }

  private var address = "BJ"

  //用this关键字定义辅助构造器
  def this(name:String,age:Int,address:String){
    //每个辅助构造器必须以主构造器或其他的辅助构造器的调用开始
    this(name,age)
    println("执行辅助构造器")
    this.address = address
  }

}
object Person{
  def main(args: Array[String]): Unit = {

    val p = new Person("dengzhou",33,"caiji")
  }
}

