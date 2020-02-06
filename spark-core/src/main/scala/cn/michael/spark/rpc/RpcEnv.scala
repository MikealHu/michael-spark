package cn.michael.spark.rpc

import cn.michael.spark.SparkConf

/**
 * Created by hufenggang on 2020/1/7.
 */
private[spark] object RpcEnv {

    def create(
                  name: String,
                  host: String,
                  port: Int,
                  conf: SparkConf,
                  securityManager: SecurityManager,
                  clientMode: Boolean = false): RpcEnv = {
        create(name, host, host, port, conf, securityManager, 0, clientMode)
    }

    def create(
                  name: String,
                  bindAddress: String,
                  advertiseAddress: String,
                  port: Int,
                  conf: SparkConf,
                  securityManager: SecurityManager,
                  numUsableCores: Int,
                  clientMode: Boolean): RpcEnv = {
        val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
            numUsableCores, clientMode)
        new NettyRpcEnvFactory().create(config)
    }

}

private[spark] abstract class RpcEnv(conf: SparkConf) {

}
