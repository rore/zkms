package im.rore.zkms

import im.rore.zkms.zkmsService._

trait ZkmsSerializer[T] {
  def serializeMessage(message:T) : Array[Byte] 
  def deserializeMessage(bytes:Array[Byte]) : T 
}

trait NoSerializer {//extends ZkmsSerializer[String] {
  def serializeMessage(message:String) : Array[Byte] = {
    if (message.isNullOrEmpty) null
    else message.toByteArray
  }
  def deserializeMessage(bytes:Array[Byte]) : String = {
    if (null == bytes) null
    else new String(bytes, "UTF-8");
  }
  		
}