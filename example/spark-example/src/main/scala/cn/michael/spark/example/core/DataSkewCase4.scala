package cn.michael.spark.example.core

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hufenggang on 2020/2/27.
 *
 * 数据倾斜优化
 *
 * 两阶段聚合
 */
object DataSkewCase4 {

  def main(args: Array[String]): Unit = {

    /** 初始化spark */
    val sparkConf = new SparkConf()
      .setAppName("DataSkewCase1")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val list = List(
      "hello me", "hello you hello hello hello",  "hello me", "hello you", "you me"
    )

    val listRDD = sc.parallelize(list)
    val random = new Random()

    // 针对异常的key添加随机前缀
    val pairs = listRDD.flatMap(x => x.split("\\s+"))
      .map(word => (random.nextInt(2) + "_" + word, 1))

    /**
     * step 1 局部聚合之添加随机前缀之后的结果
     */
    println("局部聚合之添加随机前缀之后的结果")
    println(pairs.collect().mkString("[", ",", "]"))

    /**
     * step 2 局部聚合之添加随机前缀之后的局部聚合
     */
    val partAggr = pairs.reduceByKey(_+_)
    println("局部聚合之添加随机前缀之后的局部聚合")
    println(partAggr.collect().mkString("[", ",", "]"))

    /**
     * step 4 去掉随机前缀
     */
    println("去掉随机前缀的结果")
    var parts = partAggr.map(x => (x._1.split("_")(1), x._2))
    println(parts.collect().mkString("[", ",", "]"))

    /**
     * step 5 全局聚合
     */
    val fullAggr = parts.reduceByKey(_+_)
    println("聚合后的最终结果为：")
    println(fullAggr.collect().mkString("[", ",", "]"))

    /** 释放资源 */
    sc.stop()
  }
}
