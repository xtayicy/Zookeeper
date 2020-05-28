package harry.lock;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class RemoteLock extends ProtocolSupport {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteLock.class);
	private LockListener lockListener;
	private final String dir;
	private String id;
	private String ownerId;
	private LockZookeeperOperation lockZookeeperOperation;
	private byte[] data = { 0x12, 0x34 };
	private ZNodeName zNodeName;

	public RemoteLock(ZooKeeper zooKeeper, String dir, List<ACL> acl) {
		super(zooKeeper);
		this.dir = dir;
		if (acl != null)
			setAcl(acl);

		lockZookeeperOperation = new LockZookeeperOperation();
	}

	private class LockZookeeperOperation implements ZookeeperOperation {
		private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir)
				throws KeeperException, InterruptedException {
			for (String name : zookeeper.getChildren(dir, false)) {
				if (name.startsWith(prefix)) {
					id = name;
					LOG.info("Found id created last time: " + id);
					break;
				}
			}

			if (id == null) {
				id = zookeeper.create(dir + "/" + prefix, data, getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
				LOG.info("Created id: " + id);
			}
		}

		@Override
		public boolean execute() throws KeeperException, InterruptedException {
			LOG.info("execute....");
			do {
				if (id == null) {
					long sessionId = zooKeeper.getSessionId();
					LOG.info("sessionid = " + sessionId);
					String prefix = "x-" + sessionId + "-";
					findPrefixInChildren(prefix, zooKeeper, dir);
					zNodeName = new ZNodeName(id);
				}

				if (id != null) {
					List<String> names = zooKeeper.getChildren(dir, false);
					if (names.isEmpty()) {
						LOG.warn(
								"No children in: " + dir + " when we've just " + "created one! Lets recreate it...");
						id = null;
					} else {
						SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
						for (String name : names) {
							sortedNames.add(new ZNodeName(dir + "/" + name));
						}

						ownerId = sortedNames.first().getName();
						LOG.info("ownerId = " + ownerId);
						SortedSet<ZNodeName> headSet = sortedNames.headSet(zNodeName);
						if (headSet.isEmpty()) {
							if (isOwner()) {
								if (lockListener != null) {
									lockListener.acquireLock();
								}

								return Boolean.TRUE;
							}
						} else {
							
						}
					}
				}
			} while (id == null);

			return Boolean.FALSE;
		}
	}

	public synchronized boolean lock() throws KeeperException, InterruptedException {
		if (isClosed()) {
			return false;
		}

		ensurePathExists(dir);

		return (Boolean) retryOperation(lockZookeeperOperation);
	}

	public synchronized void unlock() {
		if (!isClosed() && id != null) {
			LOG.info("unlock....");
			try {
				ZookeeperOperation zookeeperOperation = new ZookeeperOperation() {
					@Override
					public boolean execute() throws KeeperException, InterruptedException {
						zooKeeper.delete(id, -1);
						return Boolean.TRUE;
					}
				};

				zookeeperOperation.execute();
			} catch (InterruptedException e) {
				LOG.warn("Caught: " + e, e);
				Thread.currentThread().interrupt();
			} catch (KeeperException.NoNodeException e) {
			} catch (KeeperException e) {
				LOG.warn("Caught: " + e, e);
				throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
			} finally {
				if (lockListener != null) {
					lockListener.releaseLock();
				}

				id = null;
			}
		}
	}

	public boolean isOwner() {
		return id != null && ownerId != null && id.equals(ownerId);
	}

	public LockListener getLockListener() {
		return lockListener;
	}

	public void setLockListener(LockListener lockListener) {
		this.lockListener = lockListener;
	}
}
