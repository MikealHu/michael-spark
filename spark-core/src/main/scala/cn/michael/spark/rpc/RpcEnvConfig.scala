package cn.michael.spark.rpc

import cn.michael.spark.SparkConf

/**
 * Created by hufenggang on 2020/1/9.
 */
private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean)
