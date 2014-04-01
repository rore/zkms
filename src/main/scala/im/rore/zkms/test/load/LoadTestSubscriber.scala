package im.rore.zkms.test.load

import im.rore.zkms.zkmsStringService
import im.rore.zkms.zkmsService._
import org.sellmerfud.optparse._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import com.google.common.util.concurrent.AtomicDouble
import im.rore.zkms.zkmsObjectService

object LoadTestSubscriber {
  
	class MyMessage {
		var count:Int = 0;
	}

	var msgNum = new AtomicInteger
	val pool: ExecutorService = Executors.newFixedThreadPool(5)

	def task = new Callable[Unit]() {
		def call(): Unit = {
			val n = msgNum.incrementAndGet()
			System.out.print("\rgot: " + n + "               ");
		}
	}

	def messageCallback(msg: zkmsObjectService[MyMessage]#MessageReceived) {
		pool.submit(task)
	}

	def main(args: Array[String]) {
		case class Config(
			zookeeper: String = null,
			topic: String = null) {
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

		val service = new zkmsObjectService[MyMessage](config.zookeeper)
		service.subscribe(config.topic, messageCallback)
		var line: String = null;
		while ({ line = Console.readLine; line } != null) {
			if (line == "quit" || line == "q") {
				pool.shutdown()
				service.unsubscribe(config.topic)
				service.shutdown
				sys.exit(0)
			}
			if (line == "c") {
				val n = msgNum.get()
				System.out.print("\rgot for now: " + n + "               ");
				msgNum = new AtomicInteger
			}

		}
	}

}



