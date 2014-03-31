package im.rore.zkms

import im.rore.zkms.zkmsService._
import scala.reflect.runtime.universe._
import scala.reflect._
import java.io.ByteArrayOutputStream
import com.twitter.chill.ScalaKryoInstantiator

trait NoSerializer {
  def serializeMessage(message: String): Array[Byte] = {
    if (message.isNullOrEmpty) null
    else message.toByteArray
  }
  def deserializeMessage(bytes: Array[Byte]): String = {
    if (null == bytes) null
    else new String(bytes, "UTF-8");
  }
}

trait ObjectSerializer[T] {
  implicit def t: ClassTag[T]
  
  def serializeMessage(message: T): Array[Byte] = {
    if (null == message) null
    else {
      val bytes = ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(message)
      bytes
    }
  }
  def deserializeMessage(bytes: Array[Byte]): T = {
    if (null == bytes) null.asInstanceOf[T]
    else ScalaKryoInstantiator.defaultPool.fromBytes(bytes, t.runtimeClass.asInstanceOf[Class[T]])
  }
} 
  
