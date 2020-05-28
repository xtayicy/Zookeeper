package harry.cluster;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.lock.RemoteLockFuture;
import harry.lock.RemoteLockTemplate;
import harry.lock.RemoteLockUnreachableException;

/**
 * 
 * @author harry
 *
 */
public class ZookeeperLockTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLockTest.class);

	@Test
	public void test() throws IOException, RemoteLockUnreachableException, InterruptedException, ExecutionException {
		ZooKeeper zooKeeper = new ZooKeeper(ZookeeperConfig.CONNECT_STRING, 5000, null);
		RemoteLockTemplate remoteLockTemplate = new RemoteLockTemplate(zooKeeper, "/lock", 10);
		RemoteLockFuture<Object> future = remoteLockTemplate.lockAndExecute(new Callable<Object>() {

			@Override
			public Object call() throws Exception {

				return new Date();
			}
		});

		LOGGER.info(future.get().toString());
	}
}
