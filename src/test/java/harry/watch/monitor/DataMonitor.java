package harry.watch.monitor;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import harry.watch.Executor;
import harry.watch.listener.DataMonitorListener;

/**
 * 
 * @author Harry
 *
 */
public class DataMonitor implements Watcher, StatCallback {
	private ZooKeeper zooKeeper;
	private String znode;
	private DataMonitorListener listener;
	private boolean dead;

	public DataMonitor(ZooKeeper zooKeeper, String znode, Executor listener) {
		this.zooKeeper = zooKeeper;
		this.znode = znode;
		this.listener = listener;
		// Get things started by checking if the node exists.
		// We are going to be completely event driven.
		zooKeeper.exists(znode, true, this, null);
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		System.out.println("The content of the path is " + path);
		System.out.println("The type of the event is " + event.getType());
		System.out.println("The state of the event is " + event.getState());
		if (event.getType() == Event.EventType.None) {
			/**
			 * We are are being told that the state of the connection has
			 * changed
			 */
			switch (event.getState()) {
			case SyncConnected:
				/**
				 * In this particular example we don't need to do anything here
				 * - watches are automatically re-registered with server and any
				 * watches triggered while the client was disconnected will be
				 * delivered (in order of course)
				 */
				break;

			case Expired:
				// It's all over
				dead = true;
				listener.closing();
				break;

			default:
				break;
			}
		} else {
			if (path != null && path.equals(znode)) {
				// Something has changed on the node, let's find out
				zooKeeper.exists(znode, true, this, null);
			}
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctxv, Stat stat) {
		boolean exists;
		switch (rc) {
		case Code.Ok:
			System.out.println("The code is Ok.");
			exists = true;

			break;

		case Code.NoNode:
			System.out.println("The code is NoNode.");
			exists = false;

			break;

		case Code.SessionExpired:
		case Code.NoAuth:
			dead = true;
			listener.closing();

			return;

		default:
			zooKeeper.exists(znode, true, this, null);
			return;
		}
		
		byte[] b = null;
		if(exists){
			try {
				b = zooKeeper.getData(znode, false, null);
				System.out.println("The content of the b is " + new String(b));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		
		listener.exists(b);

	}

	public boolean isDead() {
		return dead;
	}
}
