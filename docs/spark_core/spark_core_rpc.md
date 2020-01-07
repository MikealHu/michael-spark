## Spark Core RPC

### 前言

### 1. Spark RPC模块的设计原理
在现在的Spark RPC中有如下的映射关系：
```
RpcEndpoint => Actor
RpcEndpointRef => ActorRef
RpcEnv => ActorSystem
```
Spark的底层通信全部采用netty进行了替换，主要使用的是spark-network-common实现，具体可参考文档：

#### 1.1 类图

