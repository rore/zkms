package im.rore.zkms;

import im.rore.zkms.zkmsStringService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class zkmsStringTest {

	private String zkConnection = "hadoop-local";
	private Boolean gotIt = false;
	
	@Before
	public void init() throws Exception {
		
	}
	
	@After
	public void teardown() throws Exception {
		
	}
	
	@Test
	public void testBroadcast() {
		zkmsStringService service = new zkmsStringService(zkConnection);
		service.subscribe("topic1", f, null);
		service.broadcast("topic1", "m1", true);
		
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

	Function1<zkmsStringService.MessageReceived, BoxedUnit> f = new AbstractFunction1<zkmsStringService.MessageReceived, BoxedUnit>() {
	    public BoxedUnit apply(zkmsStringService.MessageReceived message) {
	    	String topic = message.topic();
	    	String msg = message.message();
	    	gotIt = true;
	        return null;
	    }
	};
}
