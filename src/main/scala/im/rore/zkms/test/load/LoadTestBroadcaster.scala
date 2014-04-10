package im.rore.zkms.test.load

import im.rore.zkms.zkmsStringService
import im.rore.zkms.zkmsService._
import org.sellmerfud.optparse._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicInteger
import com.google.common.util.concurrent.AtomicDouble
import im.rore.zkms.zkmsObjectService

object LoadTestBroadcaster {
  
  class MyMessage {
    var count:Int = 0;
  }

  def main(args: Array[String]) {

    case class Config(
      zookeeper: String = null,
      topic: String = null,
      iterations: Int = 100,
      threads: Int = 2,
      message: List[String] = Nil) {
      def validate = {
        if (zookeeper.isNullOrEmpty) throw new OptionParserException("--zookeeper not specified");
        if (topic.isNullOrEmpty) throw new OptionParserException("--topic not specified");
        this
      }
    }

    val parser = new OptionParser[Config] {
      banner = "LoadBroadcaster [options] message"
      separator("")
      separator("Options:")
      reqd[String]("-z CONNECTION", "--zookeeper=CONNECTION", "Zookeeper connection string") { (v, c) => c.copy(zookeeper = v) }
      reqd[String]("-t TOPIC", "--topic=TOPIC", "Message topic") { (v, c) => c.copy(topic = v) }
      reqd[Int]("-i ITERATIONS", "--iterations=ITERATIONS", "Iterations") { (v, c) => c.copy(iterations = v) }
      reqd[Int]("-h THREADS", "--threads=THREADS", "Threads") { (v, c) => c.copy(threads = v) }
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

    val pool: ExecutorService = Executors.newFixedThreadPool(config.threads)
    val service = new zkmsObjectService[MyMessage](config.zookeeper)
    val bmessage = config.message.mkString(" ")
    val msgNum = new AtomicInteger 
    var timer = new AtomicDouble
    def doBroadcast(n:Int) = {
      val msg = new MyMessage { count = n; }
      val t0 = System.nanoTime()
      service.broadcast(config.topic, msg);
      val t1 = System.nanoTime()
      val elapsed = (t1 - t0)
      timer.addAndGet(elapsed)
    }
    def task(n:Int) = new Callable[Unit]() {
        def call(): Unit = {
          //val n = msgNum.incrementAndGet()
          doBroadcast(n)
          System.out.print("\rbroadcasting: "  + n)
        }
      }
    
    val tasks = new ListBuffer[Callable[Unit]]
    for (a <- 1 to config.iterations) {
      tasks += task(a)
    }
    val starttime = System.nanoTime()
    pool.invokeAll(tasks).map(f => f.get())
    val endtime = System.nanoTime()
    val elapsed = (endtime - starttime)
    service.shutdown
    pool.shutdown()
    val total = timer.get()
    val totalmilis = (total /  1000000)
    val elapsedmilis = (elapsed /  1000000)
    val timeperone = elapsedmilis / config.iterations
    val cando = 1000 / timeperone
    System.out.print("\ntotal time: " + elapsedmilis + " (" + totalmilis + ")" + " ms. Time per broadcast: " + timeperone + ". Can do " + cando + " per second")
  }
}