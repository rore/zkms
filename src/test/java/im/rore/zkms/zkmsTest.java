package im.rore.zkms;

import static org.junit.Assert.*;
import im.rore.zkms.MessageReceived;
import im.rore.zkms.zkmsService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class zkmsTest {

	private String zkConnection = "hadoop-local";
	
	@Before
	public void init() throws Exception {
		
	}
	
	@After
	public void teardown() throws Exception {
		
	}
	
	@Test
	public void testBroadcast() {
		zkmsService service = new zkmsService(zkConnection);
		service.subscribe("topic1", f);
		service.broadcast("topic1", "m1", false);
	}

	Function1<MessageReceived, BoxedUnit> f = new AbstractFunction1<MessageReceived, BoxedUnit>() {
	    public BoxedUnit apply(MessageReceived message) {
	    	String topic = message.topic();
	    	String msg = message.message();
	        return null;
	    }
	};
}
