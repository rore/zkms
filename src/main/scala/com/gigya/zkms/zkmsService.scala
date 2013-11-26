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
import org.apache.curator.framework.recipes.leader.LeaderSelector
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener
import org.apache.curator.framework.state.ConnectionState
import org.apache.zookeeper.KeeperException.NodeExistsException

class zkmsService(zkConnection: String) extends LeaderSelectorListener {
  import zkmsService._

  private val ZK_NAMESPACE = "zkms"
  private val CLIENTS_PATH = "/clients"
  private val SUBSCRIBERS_PATH = "/subscribers"
  private val MESSAGES_PATH = "/messages"
  private val CLEANER_LEADER_PATH = "/cleaner_leader";
  private val logger = LoggerFactory.getLogger(this.getClass());
  private val executorService: ExecutorService = Executors.newFixedThreadPool(5, ThreadUtils.newThreadFactory("zkmsThreadPool"));
  private val watchers = new ConcurrentHashMap[String, WatcherData].asScala;
  private val monitor: AutoResetEvent = new AutoResetEvent(false);
  private var _isLeader = false;
  private var cleaner: ZkmsCleaner = null;

  // create the curator zookeeper client
  logger.debug("creating zk client for: " + zkConnection)
  protected[zkms] var zkClient: CuratorFramework = CuratorFrameworkFactory.builder()
    .namespace(ZK_NAMESPACE)
    .connectString(zkConnection)
    .retryPolicy(new ExponentialBackoffRetry(200, 100))
    .build()
  // start it
  zkClient.start()
  // make sure the paths exists
  zkClient.newNamespaceAwareEnsurePath(CLIENTS_PATH).ensure(zkClient.getZookeeperClient())
  zkClient.newNamespaceAwareEnsurePath(SUBSCRIBERS_PATH).ensure(zkClient.getZookeeperClient())
  zkClient.newNamespaceAwareEnsurePath(MESSAGES_PATH).ensure(zkClient.getZookeeperClient())
  private var leaderSelector: LeaderSelector = new LeaderSelector(zkClient, CLEANER_LEADER_PATH, this);
  leaderSelector.start();

  def shutdown() {
    if (_isLeader) monitor.set;
    executorService.shutdown();
    zkClient.close();
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
    val clientPath = clientsPath(clientId)
    val subscriptionPath = subscriberPath(topic, clientId)
    val messagesPath = topicPath(topic, clientId)
    // verify we're not already subscribed
    if (watchers.contains(messagesPath)) throw new AlreadySubscribedException(topic)
    // create am ephemeral path for this client under the requested topic to mark the subscription
    createEphemeralIgnoreExists(clientPath)
    createEphemeralIgnoreExists(subscriptionPath)
    // create a listener for the messages path
    val resourcesCache = new PathChildrenCache(zkClient, messagesPath, true, false, executorService)
    resourcesCache.start(StartMode.BUILD_INITIAL_CACHE)
    val listener = new zkmsPathChildrenCacheListener(topic, messagesPath)
    resourcesCache.getListenable().addListener(listener);
    watchers.put(messagesPath, new WatcherData(resourcesCache, callback));
  }

  def unsubscribe(topic: String) {
    val subscriptionPath = subscriberPath(topic, clientId)
    val messagesPath = topicPath(topic, clientId)
    // delete our subscription and messages
    deleteRecursive(messagesPath)
    deleteRecursive(subscriptionPath)
  }

  private def createEphemeralIgnoreExists(path: String) {
    try {
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path)
    }
    catch {
      // ignore if node already exists
      case e: NodeExistsException => {}
    }
  }

  protected[zkms] def deleteRecursive(path: String) {
    if (path.isNullOrEmpty)
      throw new IllegalArgumentException("path is empty");
    zkClient.getChildren().forPath(path).asScala.foreach(child => {
      val childPath = path + "/" + child;
      deleteRecursive(childPath)
    })
    zkClient.delete().guaranteed().forPath(path)
  }

  protected[zkms] def clientsPath: String = CLIENTS_PATH
  protected[zkms] def clientsPath(clientId: String): String = clientsPath + "/" + clientId
  protected[zkms] def subscribersPath(topic: String) = SUBSCRIBERS_PATH + "/" + topic
  protected[zkms] def subscriberPath(topic: String, clientId: String) = SUBSCRIBERS_PATH + "/" + topic + "/" + clientId
  protected[zkms] def messagesPath:String = MESSAGES_PATH
  protected[zkms] def messagesPath(clientId:String):String = messagesPath + "/" + clientId 
  protected[zkms] def topicPath(topic: String, clientId: String) = messagesPath(clientId) + "/" + topic
  protected[zkms] def messagePath(topic: String, clientId: String) = topicPath(topic, clientId) + "/msg";

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

  /* (non-Javadoc)
	 * @see org.apache.curator.framework.recipes.leader.LeaderSelectorListener#takeLeadership(org.apache.curator.framework.CuratorFramework)
	 */
  protected override def takeLeadership(client: CuratorFramework) {
    logger.info("got leadership");
    // we are now the leader. This method should not return until we want to
    // relinquish leadership
    // start the leader task
    if (null == cleaner)
      cleaner = new ZkmsCleaner(this)
    cleaner.start;
    _isLeader = true;
    // wait until we loose leadership
    monitor.waitOne();
    logger.info("lost leadership");
    // we lost leadership, kill the task
    cleaner.stop;
    cleaner = null;
    _isLeader = false;
  }

  /* (non-Javadoc)
	 * @see org.apache.curator.framework.state.ConnectionStateListener#stateChanged(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.state.ConnectionState)
	 */
  protected override def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    // you MUST handle connection state changes. This WILL happen in
    // production code.
    if ((newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED)) {
      // signal that we lost the leadership
      monitor.set();
    }
  }

}

object zkmsService {
  private val clientId = UUID.randomUUID().toString()

  implicit class StringExtension(s: String) {
    def toByteArray: Array[Byte] = if (null == s) null else s.getBytes("UTF8")
    def isNullOrEmpty: Boolean = if (null == s || s.isEmpty()) true else false
  }
}

