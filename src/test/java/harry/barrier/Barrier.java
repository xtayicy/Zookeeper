package harry.barrier;

import java.util.Calendar;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.common.SyncPrimitive;

/**
 * 
 * @author Harry
 *
 */
public class Barrier extends SyncPrimitive {
	private static final Logger LOGGER = LoggerFactory.getLogger(Barrier.class);
	private int size;
	private Long timeInMillis;

	public Barrier(String address, String name, int size) {
		super(address);
		this.root = name;
		this.size = size;
		if (zooKeeper != null) {
			try {
				Stat stat = zooKeeper.exists(root, false);
				if (stat == null) {
					zooKeeper.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				LOGGER.warn("Keeper Exception when instantiating barrier: " + e.toString());
			} catch (InterruptedException e) {
				LOGGER.warn("Interrupted Exception.");
			}
			
			timeInMillis =  Calendar.getInstance().getTimeInMillis();
		}
	}

	public boolean leave() throws InterruptedException, KeeperException {
		zooKeeper.delete(root + "/" + timeInMillis, 0);
		while(true){
			synchronized (mutex) {
				List<String> list = zooKeeper.getChildren(root, true);
				if(list.size() > 0){
					mutex.wait();
				}else{
					return true;
				}
			}
		}
	}

	public boolean enter() throws KeeperException, InterruptedException {
		zooKeeper.create(root + "/" + timeInMillis, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		while(true){
			synchronized (mutex){
				List<String> list = zooKeeper.getChildren(root, true);
				if(list.size() < size){
					mutex.wait();
				}else{
					return true;
				}
			}
		}
	}
}
