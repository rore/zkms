package com.gigya.zkms

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import java.util.UUID
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.zookeeper.CreateMode
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.apache.curator.utils.ThreadUtils
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import java.util.concurrent.ConcurrentHashMap
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.utils.ZKPaths

class zkmsService(zkConnection: String) {
  import zkmsService._

  private val ZK_NAMESPACE = "zkms"
  private val SUBSCRIBERS_PATH = "/subscribers"
  private val MESSAGES_PATH = "/messages"
  private val logger = LoggerFactory.getLogger(this.getClass());
  private val executorService: ExecutorService = Executors.newFixedThreadPool(5, ThreadUtils.newThreadFactory("zkmsEventPool"));
  private val watchers = new ConcurrentHashMap[String, WatcherData].asScala;

  // create the curator zookeeper client
  logger.debug("creating zk client for: " + zkConnection)
  private var zkClient: CuratorFramework = CuratorFrameworkFactory.builder()
    .namespace(ZK_NAMESPACE)
    .connectString(zkConnection)
    .retryPolicy(new ExponentialBackoffRetry(200, 100))
    .build()
  // start it
  zkClient.start()
  // make sure the paths exists
  zkClient.newNamespaceAwareEnsurePath(SUBSCRIBERS_PATH).ensure(zkClient.getZookeeperClient())
  zkClient.newNamespaceAwareEnsurePath(MESSAGES_PATH).ensure(zkClient.getZookeeperClient())

  def send(client: String, topic: String, message: String) {

  }

  def broadcast(topic: String, message: String, sendToSelf: Boolean = false) {
    if (topic.isNullOrEmpty) throw new IllegalArgumentException("topic is null")
    if (message.isNullOrEmpty) throw new IllegalArgumentException("message is null")
    try {
      val path = subscribersPath(topic)
      // get all subscribers for topic
      val children = zkClient.getChildren().forPath(path)
      if (children.size() == 0) throw new NoSubscribersException(topic)
      children.asScala.foreach(child => {
        val msgPath = messagePath(topic, child)
        zkClient.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(msgPath, message.toByteArray)
      })
    }
    catch {
      case e: NoSubscribersException => throw e
      case e: NoNodeException => throw new NoSubscribersException(topic)
    }
  }

  def subscribe(topic: String, callback: messageCallback) {
    val subscriptionPath = subscriberPath(topic, clientId)
    val messagesPath = topicPath(topic, clientId)
    // verify we're not already subscribed
    if (watchers.contains(messagesPath)) throw new AlreadySubscribedException(topic)
    // create am ephemeral path for this client under the requested topic to mark the subscription
    zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(subscriptionPath)
    // create a listener for the messages path
    val resourcesCache = new PathChildrenCache(zkClient, messagesPath, true, false, executorService)
    resourcesCache.start(StartMode.BUILD_INITIAL_CACHE)
    val listener = new zkmsPathChildrenCacheListener(topic, messagesPath)
    resourcesCache.getListenable().addListener(listener);
    watchers.put(messagesPath, new WatcherData(resourcesCache, callback));
  }

  def unsubscribe(topic: String) {}

  private def subscribersPath(topic: String) = SUBSCRIBERS_PATH + "/" + topic
  private def subscriberPath(topic: String, clientId: String) = SUBSCRIBERS_PATH + "/" + topic + "/" + clientId
  private def topicPath(topic: String, clientId: String) = MESSAGES_PATH + "/" + clientId + "/" + topic
  private def messagePath(topic: String, clientId: String) = topicPath(topic, clientId) + "/msg";

  protected class zkmsPathChildrenCacheListener(val topic: String, val parentPath: String) extends PathChildrenCacheListener {

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
      val (message: String, path: String) = event.getType() match {
        // look for child add / update events - these are changed messages under our subscriber path
        case PathChildrenCacheEvent.Type.CHILD_ADDED | PathChildrenCacheEvent.Type.CHILD_UPDATED => {
          val fullPath = parentPath + "/" + ZKPaths.getNodeFromPath(event.getData().getPath());
          val data = new String(event.getData().getData(), "UTF-8");
          (data, fullPath)
        }
        case _ => (null, null)
      }
      // if we got a message call the callback and delete the message
      if (!message.isNullOrEmpty) {
        callCallback(parentPath, topic, message)
        // delete the message
        zkClient.delete().forPath(path)
      }
    }
  }

  type messageCallback = MessageReceived => Unit
  protected class WatcherData(val cache: PathChildrenCache, val callback: messageCallback)

  private def callCallback(watcherPath: String, topic: String, message: String) {
    try {
      submitToExecutor {
        new Runnable() {
          override def run() {
            try {
              // get the listener
              watchers.get(watcherPath) match {
                case d: Some[WatcherData] => {
                  d.get.callback(new MessageReceived(topic, message))
                }
                case None => {}
              }
            }
            catch {
              case e: Throwable => logger.error("failed calling callback", e)
            }
          }
        }
      }
    }
    catch {
      case e: Throwable => logger.error("failed calling callback", e)
    }

  }

  private def submitToExecutor(command: Runnable) {
    executorService.submit(command);
  }
}

object zkmsService {
  private val clientId = UUID.randomUUID().toString()

  implicit class StringExtension(s: String) {
    def toByteArray: Array[Byte] = if (null == s) null else s.getBytes("UTF8")
    def isNullOrEmpty: Boolean = if (null == s || s.isEmpty()) true else false
  }
}

