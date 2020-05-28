package harry.queue;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**	O
 * 
 * @author Harry
 *
 */
public class QueueTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(QueueTest.class);
	
	@Test
	public void test(){
		String address = "localhost";
		String name = "/queue";
				
		Queue queue = new Queue(address,name);
		producer(queue);
		consumer(queue);
	}
	
	private void producer(Queue queue){
		LOGGER.info("Producer: ");
		for(int i = 0;i < 10;i++){
			try {
				queue.produce(i);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void consumer(Queue queue){
		LOGGER.info("Consumer: ");
		for (int i = 0; i < 10; i++) {
			try {
				int result = queue.consume();
				System.out.println("Item: " + result);
			} catch (KeeperException e) {
				i--;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
