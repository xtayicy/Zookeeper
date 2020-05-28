package harry.barrier;

import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Harry
 *
 */
public class BarrierTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(BarrierTest.class);
	
	@Test
	public void test(){
		String address = "localhost";
		String name = "/barrier";
		int size = 2;
		Barrier barrier = new Barrier(address, name, size);
		try {
			boolean flag = barrier.enter();
			if(!flag) LOGGER.warn("Error when entering the barrier.");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Random random = new Random();
		int r = random.nextInt(100);
		for (int i = 0; i < r; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			barrier.leave();
		} catch (InterruptedException | KeeperException e) {
		}
		
		LOGGER.info("Left barrier.");
	}
}
