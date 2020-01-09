package cn.michael.spark.example.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2020/1/3 9:44
 */
object SparkContextTest {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
            .setAppName("sparkContext-test")
            .setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.parallelize(List("java hadoop spark scala"))

        val result = rdd
            .flatMap {x => x.split(" ")}
            .map((_, 1))
            .reduceByKey(_ + _)

        result.foreach(x => println(x))

        sc.stop()
    }

}
