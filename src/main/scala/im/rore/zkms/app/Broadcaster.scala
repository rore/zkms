package im.rore.zkms.app

import im.rore.zkms.zkmsService
import im.rore.zkms.zkmsService._
import org.sellmerfud.optparse._

object Broadcaster {

  def main(args: Array[String]) {
    
    case class Config(
      zookeeper: String = null,
      topic: String = null,
      message: List[String] = Nil) {
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
      args[String] { (v, c) => c.copy(message = v) }
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
    service.broadcast(config.topic, config.message.mkString(" "));
    service.shutdown
  }
}