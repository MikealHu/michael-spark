package cn.michael.spark.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hufenggang on 2020/2/8.
 */
object SparkStreamingExample01 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
            .setAppName("spark-streaming-example01")
            .setMaster("local[2]")

        val ssc = new StreamingContext(conf, Seconds(1))
        ssc.sparkContext.setLogLevel("warn")

        val lines = ssc.socketTextStream("localhost", 9999)

        val words = lines.flatMap(_.split(","))
        val pairs = words.map(x => (x, 1))
        val wordCounts = pairs.reduceByKey(_+_)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
