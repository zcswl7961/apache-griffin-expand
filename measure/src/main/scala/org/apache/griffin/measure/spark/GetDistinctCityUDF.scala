package org.apache.griffin.measure.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 * 自定义的UDF函数
 *    自定义一个UDF函数需要继承UserDefinedAggregateFunction类，并实现其中的8个方法
 * zhoucg
 * 2019-10-15
 */
object GetDistinctCityUDF extends UserDefinedAggregateFunction{
  /**
   * 输入数据类型
   * @return
   */
  override def inputSchema: StructType = StructType(
    StructField("status",StringType,true) :: Nil
  )

  /**
   * 缓存字段类型
   * */
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("buffer_city_info",StringType,true)
      )
    )
  }
  /**
   * 输出结果类型
   * */
  override def dataType: DataType = StringType
  /**
   * 输入类型和输出类型是否一致
   * */
  override def deterministic: Boolean = true
  /**
   * 对辅助字段进行初始化
   * */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }
  /**
   *修改辅助字段的值
   * */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取最后一次的值
    var last_str = buffer.getString(0)
    //获取当前的值
    val current_str = input.getString(0)
    //判断最后一次的值是否包含当前的值
    if(!last_str.contains(current_str)){
      //判断是否是第一个值，是的话走if赋值，不是的话走else追加
      if(last_str.equals("")){
        last_str = current_str
      }else{
        last_str += "," + current_str
      }
    }
    buffer.update(0,last_str)

  }
  /**
   *对分区结果进行合并
   * buffer1是机器hadoop1上的结果
   * buffer2是机器Hadoop2上的结果
   * */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var buf1 = buffer1.getString(0)
    val buf2 = buffer2.getString(0)
    //将buf2里面存在的数据而buf1里面没有的数据追加到buf1
    //buf2的数据按照，进行切分
    for(s <- buf2.split(",")){
      if(!buf1.contains(s)){
        if(buf1.equals("")){
          buf1 = s
        }else{
          buf1 += s
        }
      }
    }
    buffer1.update(0,buf1)
  }
  /**
   * 最终的计算结果
   * */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
