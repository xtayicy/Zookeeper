package harry.queue;

import java.nio.ByteBuffer;
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
public class Queue extends SyncPrimitive{
	private Logger LOGGER = LoggerFactory.getLogger(Queue.class);
	
	Queue(String address,String name){
		super(address);
		this.root = name;
		if(zooKeeper != null){
			try {
				Stat stat = zooKeeper.exists(root, false);
				if(stat == null){
					zooKeeper.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				LOGGER.info("Keeper Exception when instantiating queue: " + e.toString());
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted Exception.");
			}
		}
	}
	
	public int consume() throws KeeperException, InterruptedException {
		int returnVal = -1;
		
		while(true){
			synchronized (mutex) {
				List<String> list = zooKeeper.getChildren(root, true);
				if(list.size() == 0){
					LOGGER.info("Going to wait.");
					mutex.wait();
				}else{
					String minNode = list.get(0);
					Integer min = new Integer(minNode.substring(7));
					for (String s : list) {
						Integer tempValue = new Integer(s.substring(7));
						if(tempValue < min){
							min = tempValue;
							minNode = s;
						}
					}
					
					LOGGER.info("Temporary value: " + root + "/" + minNode);
					byte[] b = zooKeeper.getData(root + "/" + minNode, false, null);
					zooKeeper.delete(root + "/" + minNode, 0);
					ByteBuffer buffer = ByteBuffer.wrap(b);
					returnVal = buffer.getInt();
					
					return returnVal;
				}
			}
		}
	}

	public boolean produce(int i) throws KeeperException, InterruptedException{
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(i);
		
		byte[] value;
		value = buffer.array();
		zooKeeper.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		return true;
	}
}
