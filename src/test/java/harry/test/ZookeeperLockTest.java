package harry.test;

import java.io.IOException;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.test.lock.RemoteLockFuture;
import harry.test.lock.RemoteLockTemplate;
import harry.test.lock.RemoteLockUnreachableException;
import harry.test.lock.ZNodeName;

/**
 * 
 * @author harry
 *
 */
public class ZookeeperLockTest {
	private static final Logger logger = LoggerFactory.getLogger(ZookeeperLockTest.class);

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

		logger.info(future.get().toString());
	}

	@Test
	public void test_() {

	}
}
