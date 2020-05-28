package harry.common;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Harry
 *
 */
public class SyncPrimitive implements Watcher{
	protected static ZooKeeper zooKeeper;
	protected static Integer mutex;
	protected String root;
	private static final Logger LOGGER = LoggerFactory.getLogger(SyncPrimitive.class);
	
	public SyncPrimitive(String address) {
		if(zooKeeper == null){
			try {
				LOGGER.info("Starting ZooKeeper: ");
				zooKeeper = new ZooKeeper(address, 3000, this);
				mutex = new Integer(-1);
				System.out.println("Fininshed starting ZooKeeper: " + zooKeeper);
			} catch (IOException e) {
				LOGGER.warn(e.toString());
				zooKeeper = null;
			}
		}
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}

}
