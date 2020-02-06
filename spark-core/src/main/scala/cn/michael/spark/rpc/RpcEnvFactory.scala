package cn.michael.spark.rpc

/**
 * Created by hufenggang on 2020/1/9.
 */
private[spark] trait RpcEnvFactory {

    def create(config: RpcEnvConfig): RpcEnv

}
