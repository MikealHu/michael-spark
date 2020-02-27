package cn.michael.spark.example.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hufenggang on 2020/2/27.
 *
 * 数据倾斜优化
 *
 * 过滤发生数据倾斜的key
 */
object DataSkewCase1 {

  def main(args: Array[String]): Unit = {

    /** 初始化spark */
    val sparkConf = new SparkConf()
      .setAppName("DataSkewCase1")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val list = List(
      "hello kang kang kang",
      "hello feng kang kang",
      "kang kang feng you",
      "yan kang feng kang",
      "kang kang you 1",
      "kang kang me shit",
      "hello kang kang kang",
      "hello feng kang kang",
      "kang kang feng you",
      "yan kang feng kang",
      "kang kang you 1",
      "kang kang me shit"
    )

    val listRDD = sc.parallelize(list)
    val wordOneRDD = listRDD.flatMap(x => x.split(" ")).map(x => (x, 1))

    /**
     * 确定发生数据倾斜的key，找到数据最多的key
     */
    val sampleRDD = wordOneRDD.sample(true, 0.6)
    val sortedRDD = sampleRDD.reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false, 1)

    println("===抽样结果打印===")
    sortedRDD.foreach(x => println(x))
    println("==发生数据倾斜的key==")
    val dataskewKeys = sortedRDD.take(1).map(x => x._2)
    dataskewKeys.foreach(x => println(x))

    /**
     * 从原始数据中过滤掉发生数据倾斜的key
     */
    val bc = sc.broadcast(dataskewKeys)
    val filterRDD = wordOneRDD.filter(x => {
      var tag = true
      for (word <- bc.value) {
        if (x._1.equals(word)) {
          tag =  false
        }
      }
      tag
    })
    val result = filterRDD.reduceByKey(_ + _)
    println("==过滤数据倾斜的key之后的结果==")
    result.foreach(x => println(x))

    /** 释放资源 */
    sc.stop()
  }

}
