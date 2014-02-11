package com.gigya.zkms.app

import com.gigya.zkms.zkmsService
import com.gigya.zkms.zkmsService._
import org.sellmerfud.optparse._
import com.gigya.zkms.MessageReceived

object Subscriber {

  def main(args: Array[String]) {
    
    case class Config(
      zookeeper: String = null,
      topic: String = null
      ) {
      def validate = {
        if (zookeeper.isNullOrEmpty) throw new OptionParserException("--zookeeper not specified");
        if (topic.isNullOrEmpty) throw new OptionParserException("--topic not specified");
        this
      }
    }

    val parser = new OptionParser[Config] {
      banner = "Broadcaster [options] message"
      separator("")
      separator("Options:")
      reqd[String]("-z CONNECTION", "--zookeeper=CONNECTION", "Zookeeper connection string") { (v, c) => c.copy(zookeeper = v) }
      reqd[String]("-t TOPIC", "--topic=TOPIC", "Message topic") { (v, c) => c.copy(topic = v) }
      
    }
    val config = try {
      parser.parse(args, Config()).validate
    }
    catch {
      case e: OptionParserException => {
        println(e.getMessage + ". Usage:\n" + parser); sys.exit(1)
      }
    }

    val service = new zkmsService(config.zookeeper)
    service.subscribe(config.topic, message)
    var line:String=null;
    while ({line = Console.readLine; line} != null){
      if (line == "quit" || line == "q") {
        service.unsubscribe(config.topic)
        service.shutdown
        sys.exit(0)
      }
    } 
  }
  
  def message(msg:MessageReceived) {
    println(msg.message);
  }
}