package cn.michael.spark.example.hudi.test

import cn.michael.spark.common.enums.LoggerLeverlEnum
import cn.michael.spark.common.utils.SparkSQLUtils
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.SparkSession

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 15:33
 *
 * spark查询Hudi数据
 */
object SparkHudiSelect {

    val tableName = "hudi_spark_test"
    val basePath = "tmp/hudi_cow_table"
    val dataGen = new DataGenerator

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkSQLUtils.getSparkSession("Spark-hudi-test")
        sparkSession.sparkContext.setLogLevel(LoggerLeverlEnum.DEBUG.value)

        val df = sparkSession.read.
            format("org.apache.hudi").
            load(basePath + "/*/*/*/*")

        df.createOrReplaceGlobalTempView("v_hudi_spark_test")

        import sparkSession.sql
        sql(
            s"""
              |select fare, begin_lon, begin_lat, ts from  v_hudi_spark_test where fare > 20.0
              |""".stripMargin).show()

        sql(
            s"""
              |select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  v_hudi_spark_test
              |""".stripMargin).show()

        sparkSession.stop()
    }

}
