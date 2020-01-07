package cn.michael.spark.example.core

import org.apache.spark.rdd.RDD
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
    }

}
