package cn.michael.spark.serializer

import java.io._
import java.nio.ByteBuffer

import cn.michael.spark.annotation.{DeveloperApi, Private}

import scala.reflect.ClassTag

/**
 * created by hufenggang on 2020/1/6
 *
 * :: DeveloperApi ::
 * 序列化器。
 * 由于一些序列化库是线程非安全的，所以该类用于创建一个[[cn.michael.spark.serializer.SerializerInstance]]对象
 * 用于实现序列化并且保证在同一个线程中只能被调用一次。
 *
 * 该特征的实现类需要实现如下接口：
 * 1. 无参构造方法或者参数为[[cn.michael.spark.SparkConf]]的构造方法，如果两个方法都定义了，后者优先；
 * 2. Java serialization interface.
 */

@DeveloperApi
abstract class Serializer {

    /**
     * 用于反序列化的默认ClassLoader。
     * [[Serializer]]的实现应确保设置是正在使用它。
     */
    @volatile protected var defaultClassLoader: Option[ClassLoader] = None

    /**
     * 为序列化程序设置类加载器，以在反序列化时使用。
     *
     * @param classLoader
     * @return
     */
    def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
        defaultClassLoader = Some(classLoader)
        this
    }

    /** 创建一个新的[[SerializerInstance]]对象 */
    def newInstance(): SerializerInstance

    /**
     * :: Private ::
     * 如果此序列化器支持对序列化对象的重定位，则返回true，否则返回false。
     * 当且仅当序列化流输出中对序列化对象的字节重新排序等效于在对它们进行序列化之前对它们进行
     * 重新排序时，才应返回true。更具体说：如果序列化器支持重定位，则应满足以下条件：

     * {{{
     * serOut.open()
     * position = 0
     * serOut.write(obj1)
     * serOut.flush()
     * position = # of bytes written to stream so far
     * obj1Bytes = output[0:position-1]
     * serOut.write(obj2)
     * serOut.flush()
     * position2 = # of bytes written to stream so far
     * obj2Bytes = output[position:position2-1]
     * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
     * }}}
     *
     * @return
     */
    @Private
    private[spark] def supportsRelocationOfSerializedObjects: Boolean = false

}

@DeveloperApi
abstract class SerializerInstance {
    def serialize[T: ClassTag](t: T): ByteBuffer

    def deserialize[T: ClassTag](bytes: ByteBuffer): T

    def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

    def serializeStream(s: OutputStream): SerializationStream

    def deserializeStream(s: InputStream): DeserializationStream
}

/**
 * 序列化流
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
abstract class SerializationStream extends Closeable {

    /** 编写对象的最通用方法 */
    def writeObject[T: ClassTag](t: T): SerializationStream

    /** 写入表示键值对的键的对象 */
    def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)

    def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)

    def flush(): Unit

    override def close(): Unit

    def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
        while (iter.hasNext) {
            writeObject(iter.next())
        }
        this
    }
}

/**
 * 反序列化流
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
@DeveloperApi
abstract class DeserializationStream extends Closeable {

    /** 读取对象的最通用方法 */
    def readObject[T: ClassTag](): T

    /** 读取表示键值对的键的对象 */
    def readKey[T: ClassTag](): T = readObject[T]()

    def readValue[T: ClassTag](): T = readObject[T]()

    override def close(): Unit

    def asIterator: Iterator[Any] = new NextIterator[Any] {

        override def getNext = {
            try {
                readObject[Any]()
            } catch {
                case eof: EOFException =>
                    finished = true
                    null
            }
        }

        override protected def close(): Unit = {
            DeserializationStream.this.close()
        }
    }

    /**
     * 通过迭代器在key-value键值对上读取流中一个元素。该方法
     * @return
     */
    def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {

        override protected def getNext() = {
            try {
                (readKey[Any](), readValue[Any]())
            } catch {
                case eof: EOFException =>
                    finished = true
                    null
            }
        }

        override protected def close(): Unit = {
            DeserializationStream.this.close()
        }
    }
}
