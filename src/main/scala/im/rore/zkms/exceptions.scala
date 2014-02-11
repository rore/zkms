package com.gigya.zkms

class ZkmsException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}

class NoSubscribersException(val topic: String) extends ZkmsException("No subscribers for topic: " + topic) {
}

class AlreadySubscribedException(val topic: String) extends ZkmsException("Already subscribed for topic: " + topic) {
}