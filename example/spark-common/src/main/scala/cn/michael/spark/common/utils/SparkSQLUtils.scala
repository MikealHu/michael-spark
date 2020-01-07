package cn.michael.spark.common.utils

import cn.michael.spark.common.constant.SparkConfConstant
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 13:52
 *
 */
object SparkSQLUtils {

    val KryoSerializer = "org.apache.spark.serializer.KryoSerializer"

    /**
     * Spark项目初始化，获取对应的SparkSession对象
     *
     * @param sparkConf SparkConf对象
     * @return SparkSession对象
     */
    def getSparkSession(sparkConf: SparkConf): SparkSession = {
        val sparkConfigMap = ConfigUtils.getPropertiesMap(this.getClass, "/userprofile-spark.properties")

        // 判断是否本地调试环境
        val sparkSession = if (
            sparkConfigMap.contains(SparkConfConstant.SPARK_ENV_CONFIG_KEY)
                && sparkConfigMap(SparkConfConstant.SPARK_ENV_CONFIG_KEY).equals("dev")
        ) {
            SparkSession
                .builder()
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate()
        } else {
            SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate()
        }

        sparkSession
    }

    /**
     * Spark项目初始化，获取对应的SparkSession对象
     *
     * @param sparkConf SparkConf对象
     * @param classes   需要Kryo序列化的类
     * @return SparkSession对象
     */
    def getSparkSessionWithKryoClasses(sparkConf: SparkConf, classes: Array[Class[_]]): SparkSession = {

        if (classes != null && !classes.isEmpty) {
            sparkConf.registerKryoClasses(classes)
        }

        val sparkConfigMap = ConfigUtils.getPropertiesMap(this.getClass, "/userprofile-spark.properties")

        // 判断是否本地调试环境
        val sparkSession = if (
            sparkConfigMap.contains(SparkConfConstant.SPARK_ENV_CONFIG_KEY)
                && sparkConfigMap(SparkConfConstant.SPARK_ENV_CONFIG_KEY).equals("dev")
        ) {
            SparkSession
                .builder()
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate()
        } else {
            SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate()
        }

        sparkSession
    }

    /**
     * Spark项目初始化，获取对应的SparkSession对象
     *
     * @param appName AppName
     * @return SparkSession对象
     */
    def getSparkSession(appName: String): SparkSession = {

        val sparkConf = new SparkConf()
            .setAppName(appName)
            .set("spark.serializer", KryoSerializer)

        val sparkConfigMap = ConfigUtils.getPropertiesMap(this.getClass, "/dev/michael-spark-conf.properties")

        // 判断是否本地调试环境
        val sparkSession = if (
            sparkConfigMap.contains(SparkConfConstant.SPARK_ENV_CONFIG_KEY)
                && sparkConfigMap(SparkConfConstant.SPARK_ENV_CONFIG_KEY).equals("dev")
        ) {
            SparkSession
                .builder()
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate()
        } else {
            SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate()
        }

        sparkSession
    }

    /**
     * Spark项目初始化，获取对应的SparkSession对象
     *
     * @param appName AppName
     * @param classes 需要Kryo序列化的类
     * @return SparkSession对象
     */
    def getSparkSessionWithKryoClasses(appName: String, classes: Array[Class[_]]): SparkSession = {

        val sparkConf = new SparkConf()
            .setAppName(appName)
            .set("spark.serializer", KryoSerializer)
            .set("spark.kryo.registrationRequired", "true")

        if (classes != null && !classes.isEmpty) {
            sparkConf.registerKryoClasses(classes)
        }

        val sparkConfigMap = ConfigUtils.getPropertiesMap(this.getClass, "/userprofile-spark.properties")

        // 判断是否本地调试环境
        val sparkSession = if (
            sparkConfigMap.contains(SparkConfConstant.SPARK_ENV_CONFIG_KEY)
                && sparkConfigMap(SparkConfConstant.SPARK_ENV_CONFIG_KEY).equals("dev")
        ) {
            SparkSession
                .builder()
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate()
        } else {
            SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate()
        }

        sparkSession
    }
}
