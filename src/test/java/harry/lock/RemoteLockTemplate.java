package harry.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author harry
 *
 */
public class RemoteLockTemplate {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteLockTemplate.class);
	private static final AtomicLong COUNTER = new AtomicLong(0);
	private static final TreeMap<Long, RemoteLockFuture<?>> TREEMAP = new TreeMap<Long, RemoteLockFuture<?>>();
	private Lock lock = new ReentrantLock();
	private RemoteLock remoteLock;
	private Integer batchSize = 5;

	public RemoteLockTemplate(ZooKeeper zooKeeper, String lockPath, int tasks) {
		remoteLock = new RemoteLock(zooKeeper, lockPath, null);
		remoteLock.setLockListener(new LockListener() {

			@SuppressWarnings("unchecked")
			@Override
			public void acquireLock() {
				LOG.info("acquire lock...");

				try {
					List<RemoteLockFuture<?>> futures = new ArrayList<RemoteLockFuture<?>>();
					lock.lock();
					try {
						int min = Math.min(TREEMAP.size(), batchSize);
						for (int i = 0; i < min; i++) {
							Entry<Long, RemoteLockFuture<?>> pollFirstEntry = TREEMAP.pollFirstEntry();
							if (pollFirstEntry != null) {
								RemoteLockFuture<?> value = pollFirstEntry.getValue();
								if (!value.isCancelled()) {
									futures.add(value);
								}
							}
						}
					} finally {
						lock.unlock();
					}

					if (!futures.isEmpty()) {
						for (@SuppressWarnings("rawtypes") RemoteLockFuture remoteLockFuture : futures) {
							try {
								remoteLockFuture.doneWithResult(remoteLockFuture.getCallable().call());
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				} finally {
					remoteLock.unlock();
				}
			}

			@Override
			public void releaseLock() {
				LOG.info("release lock.....");
			}
		});
	}

	public <V> RemoteLockFuture<V> lockAndExecute(Callable<V> callable) throws RemoteLockUnreachableException {
		LOG.info("lockAndExecute...");
		RemoteLockFuture<V> remoteLockFuture = null;
		lock.lock();
		long sn = COUNTER.incrementAndGet();
		try {
			remoteLockFuture = new RemoteLockFuture<>(sn, TREEMAP, callable);
			TREEMAP.put(sn, remoteLockFuture);
			LOG.info("treeMap :" + TREEMAP);
		} finally {
			lock.unlock();
		}

		try {
			LOG.info("lock...");
			remoteLock.lock();
		} catch (Exception e) {
			lock.lock();
			try {
				TREEMAP.remove(sn);
			} finally {
				lock.unlock();
			}

			throw new RemoteLockUnreachableException(e);
		}

		return remoteLockFuture;
	}
	
	public static void main(String[] args) throws IOException, RemoteLockUnreachableException, InterruptedException, ExecutionException {
		
	}
}
