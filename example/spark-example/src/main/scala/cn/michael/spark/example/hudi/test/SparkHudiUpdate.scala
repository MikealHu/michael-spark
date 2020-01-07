package cn.michael.spark.example.hudi.test

import cn.michael.spark.common.enums.LoggerLeverlEnum
import cn.michael.spark.common.utils.SparkSQLUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 17:50
 *
 */
object SparkHudiUpdate {

    val tableName = "hudi_spark_test"
    val basePath = "tmp/hudi_cow_table"
    val dataGen = new DataGenerator

    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkSQLUtils.getSparkSession("SparkHudiInsert")
        sparkSession.sparkContext.setLogLevel(LoggerLeverlEnum.DEBUG.value)

        val updates = convertToStringList(dataGen.generateUpdates(10))
        val df = sparkSession.read.json(sparkSession.sparkContext.parallelize(updates, 2));
        df.write.format("org.apache.hudi").
            options(getQuickstartWriteConfigs).
            option(PRECOMBINE_FIELD_OPT_KEY, "ts").
            option(RECORDKEY_FIELD_OPT_KEY, "uuid").
            option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
            option(TABLE_NAME, tableName).
            mode(Append).
            save(basePath);
    }

}
