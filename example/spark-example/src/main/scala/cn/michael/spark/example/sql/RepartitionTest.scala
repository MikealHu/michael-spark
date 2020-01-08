package cn.michael.spark.example.sql

import org.apache.spark.sql.SparkSession

/**
 * Created by hufenggang on 2020/1/8.
 */
object RepartitionTest {

    def main(args: Array[String]): Unit = {
        val list = List(
            Student(1, "hu", 22),
            Student(2, "feng", 23),
            Student(2, "gang", 24)
        )

        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("test")
            .getOrCreate()

        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(list)

        val df = rdd.toDF()

        df.show()

        println("------->> <<-------")

        val result = df.repartition(1, org.apache.spark.sql.functions.rand())

        df.repartition(1)

        result.show()

    }

}


private[sql] case class Student(
    id: Int,
    Name: String,
    age: Int)
