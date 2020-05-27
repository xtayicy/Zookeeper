package harry.barrier;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import harry.common.SyncPrimitive;

/**
 * 
 * @author Harry
 *
 */
public class Barrier extends SyncPrimitive {
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
				System.out.println("Keeper Exception when instantiating barrier: " + e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception.");
			}
			
			timeInMillis =  Calendar.getInstance().getTimeInMillis();
		}
	}

	public static void main(String[] args) {
		String address = "localhost";
		String name = "/barrier";
		int size = 2;
		Barrier barrier = new Barrier(address, name, size);
		try {
			boolean flag = barrier.enter();
			if(!flag) System.out.println("Error when entering the barrier.");
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
		
		System.out.println("Left barrier.");
	}

	private boolean leave() throws InterruptedException, KeeperException {
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

	private boolean enter() throws KeeperException, InterruptedException {
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
