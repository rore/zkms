zkms - A simple messaging service based on zookeeper 
====
This library intends to answer a simple need - broadcasting messages between nodes of a distributed service that is coordinated on top of [zookeeper](http://zookeeper.apache.org/). It is written in scala and can be used from scala or Java.

Motivation
----
There are cases where you want to broadcast messages to all the nodes running a distributed service (for instance - when you need to remove an item from a local memory cache that is present on every node), but you don't need (or want) the overhead of managing a full blown message queue.

If this service is coordinated via zookeeper, it can make sense to utilize zookeeper also for managing the messages between the nodes.

This library implements such a messaging service that runs on zookeeper. Since this is not the primary or intended use for zookeeper it is not built to handle high loads. So use this library with care, and only under the following rules of thumb:

Use this library if you expect:

- Low message rate.
- A medium size cluster of nodes (a few to a few dozens of nodes / subscribers).

**Don't** use this if you expect high message throughput or a lot of subscribers. Performance will probably *suck*.   
See this [netflix note](https://github.com/Netflix/curator/wiki/Tech-Note-4) for additional information on possible issues with using zookeeper for messaging. 

##Usage
To use zkms as a maven dependency add the following repository and dependency:
```
<repositories>
	<repository>
				<id>rore-releases</id>
				<url>https://github.com/rore/rore-repo/raw/master/releases</url>
	</repository>
</repositories>

<dependencies>
	<dependency>
				<groupId>im.rore</groupId>
				<artifactId>zkms</artifactId>
				<version>0.0.1-SNAPSHOT</version>
	</dependency>
</dependencies>
```
The class **zkmsService** implements all the methods for using the messaging service.

###Broadcasting a message
Messages are broadcasted into "topics" - an arbitrary string used to group messages together. 

Example (in real life use you will keep the zkms service instance across the lifetime of the application and will only shut it down on exit):
```scala
val service = new zkmsService(zkConnection);
service.broadcast("mytopic", "hello world!", false);
service.shutdown();
```

###Subscribing to messages
Subscription is done to a specific topic. A client can subscribe to multiple topics.
Example:

scala:
```scala
def messageCallback(msg:MessageReceived) {
   println(msg.message);
}
val service = new zkmsService(zkConnection)
service.subscribe(topic, messageCallback)
```

java (in Java defining the callback is a bit uglier):
```java
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

Function1<MessageReceived, BoxedUnit> messageCallback = new AbstractFunction1<MessageReceived, BoxedUnit>() {
	    public BoxedUnit apply(MessageReceived message) {
	    	String topic = message.topic();
	    	String msg = message.message();
	    	// Do your thing here
	        return null;
    }
};

public void subscribe() {	
	zkmsService service = new zkmsService(zkConnection);
	service.subscribe("topic1", messageCallback);
}
```
