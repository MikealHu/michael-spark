package cn.michael.spark.example.hudi.test

import cn.michael.spark.common.enums.LoggerLeverlEnum
import cn.michael.spark.common.utils.SparkSQLUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConversions._

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 11:51
 *
 * spark插入数据到Hudi
 */
object SparkHudiInsert {

    val tableName = "hudi_spark_test"
    val basePath = "file:///tmp/hudi_cow_table"
    val dataGen = new DataGenerator

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkSQLUtils.getSparkSession("SparkHudiInsert")
        sparkSession.sparkContext.setLogLevel(LoggerLeverlEnum.DEBUG.value)

        val inserts = convertToStringList(dataGen.generateInserts(10))
        val df: DataFrame = sparkSession.read.json(sparkSession.sparkContext.parallelize(inserts, 2))
        df.write.format("org.apache.hudi").
            options(getQuickstartWriteConfigs).
            option(PRECOMBINE_FIELD_OPT_KEY, "ts").
            option(RECORDKEY_FIELD_OPT_KEY, "uuid").
            option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
            option(TABLE_NAME, tableName).
            mode(Overwrite).
            save(basePath)

        sparkSession.stop()

    }

}
