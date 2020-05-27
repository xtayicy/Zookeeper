package harry.queue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import harry.common.SyncPrimitive;

/**
 * 
 * @author Harry
 *
 */
public class Queue extends SyncPrimitive{
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
				System.out.println("Keeper Exception when instantiating queue: " + e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception.");
			}
		}
	}
	
	public static void main(String[] args) {
		String address = "localhost";
		String name = "/queue";
				
		Queue queue = new Queue(address,name);
		producer(queue);
		//consumer(queue);
	}
	
	public static void consumer(Queue queue){
		System.out.println("Consumer: ");
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
	
	private int consume() throws KeeperException, InterruptedException {
		int returnVal = -1;
		
		while(true){
			synchronized (mutex) {
				List<String> list = zooKeeper.getChildren(root, true);
				if(list.size() == 0){
					System.out.println("Going to wait.");
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
					
					System.out.println("Temporary value: " + root + "/" + minNode);
					byte[] b = zooKeeper.getData(root + "/" + minNode, false, null);
					zooKeeper.delete(root + "/" + minNode, 0);
					ByteBuffer buffer = ByteBuffer.wrap(b);
					returnVal = buffer.getInt();
					
					return returnVal;
				}
			}
		}
	}

	public static void producer(Queue queue){
		System.out.println("Producer: ");
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
	
	private boolean produce(int i) throws KeeperException, InterruptedException{
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(i);
		
		byte[] value;
		value = buffer.array();
		zooKeeper.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		return true;
	}
}
