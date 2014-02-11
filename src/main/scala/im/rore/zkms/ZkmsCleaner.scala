package im.rore.zkms

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.curator.utils.ZKPaths

/**
 * Leader task for doing cleanup on abandoned subscribers.
 * Removed the messages for subscribers that are no longer connected.
 *
 * @author Rotem Hermon
 */
protected class ZkmsCleaner(service: zkmsService) {
  // run the cleaner every X seconds
  val TASK_RATE_SECONDS = 60
  
  private val logger = LoggerFactory.getLogger(this.getClass());
  var executor: ScheduledExecutorService = null;
  
  def start() {
    if (null == executor) {
      executor = Executors.newSingleThreadScheduledExecutor()
      executor.scheduleAtFixedRate(new Runnable {
        def run() {
          logger.debug("ZkmsCleaner - run")
          doCleanup
        }
      }, 1, TASK_RATE_SECONDS, TimeUnit.SECONDS)
    }
    else throw new RuntimeException("executor has already started");
  }

  def stop() {
    executor.shutdown();
    executor = null;
  }

  protected def doCleanup {
    try {
      // go over the message path and look for queues that has no connected subscribers
      val children = service.zkClient.getChildren().forPath(service.messagesPath)
      children.asScala.foreach(child => {
        // check that we still have this subscriber
        val clientPath = service.clientsPath(child)
        val stats = service.zkClient.checkExists().forPath(clientPath)
        if (null == stats){
          // this client is not connected, so remove the messages
          val msgPath = service.messagesPath(child)
          service.deleteRecursive(msgPath)
        }
      })

    }
    catch {
      case e: Throwable => logger.error("while doing cleanup", e)
    }
  }
}