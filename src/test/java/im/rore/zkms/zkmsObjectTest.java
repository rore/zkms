package im.rore.zkms;

import im.rore.zkms.zkmsObjectService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class zkmsObjectTest {

	private String zkConnection = "hadoop-local";
	private Boolean gotIt = false;
	
	@Before
	public void init() throws Exception {
		
	}
	
	@After
	public void teardown() throws Exception {
		
	}
	
	public class MyMessage {
		public String Message;
	}
	
	@Test
	public void testBroadcast() {
		MyMessage msg = new MyMessage();
		msg.Message = "this is my message";
		
		zkmsObjectService<MyMessage> service = new zkmsObjectService<MyMessage>(zkConnection, MyMessage.class);
		service.subscribe("topic1", f);
		service.broadcast("topic1", msg, true);
		
		int tries = 0;
		while (!gotIt && tries < 10) {
			tries ++;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	Function1<zkmsObjectService<MyMessage>.MessageReceived, BoxedUnit> f = new AbstractFunction1<zkmsObjectService<MyMessage>.MessageReceived, BoxedUnit>() {
	    public BoxedUnit apply(zkmsObjectService<MyMessage>.MessageReceived message) {
	    	String topic = message.topic();
	    	MyMessage msg = message.message();
	    	gotIt = true;
	        return null;
	    }
	};
}
