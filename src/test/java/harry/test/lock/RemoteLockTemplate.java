package harry.test.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
	private static final Logger logger = LoggerFactory.getLogger(RemoteLockTemplate.class);
	private static final AtomicLong counter = new AtomicLong(0);
	private static final TreeMap<Long, RemoteLockFuture<?>> treeMap = new TreeMap<Long, RemoteLockFuture<?>>();
	private Lock lock = new ReentrantLock();
	private RemoteLock remoteLock;
	private Integer batchSize = 5;

	public RemoteLockTemplate(ZooKeeper zooKeeper, String lockPath, int tasks) {
		remoteLock = new RemoteLock(zooKeeper, lockPath, null);
		remoteLock.setLockListener(new LockListener() {

			@SuppressWarnings("unchecked")
			@Override
			public void acquireLock() {
				logger.info("acquire lock...");

				try {
					List<RemoteLockFuture<?>> futures = new ArrayList<RemoteLockFuture<?>>();
					lock.lock();
					try {
						int min = Math.min(treeMap.size(), batchSize);
						for (int i = 0; i < min; i++) {
							Entry<Long, RemoteLockFuture<?>> pollFirstEntry = treeMap.pollFirstEntry();
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
				logger.info("release lock.....");
			}
		});
	}

	public <V> RemoteLockFuture<V> lockAndExecute(Callable<V> callable) throws RemoteLockUnreachableException {
		logger.info("lockAndExecute...");
		RemoteLockFuture<V> remoteLockFuture = null;
		lock.lock();
		long sn = counter.incrementAndGet();
		try {
			remoteLockFuture = new RemoteLockFuture<>(sn, treeMap, callable);
			treeMap.put(sn, remoteLockFuture);
			logger.info("treeMap :" + treeMap);
		} finally {
			lock.unlock();
		}

		try {
			logger.info("lock...");
			remoteLock.lock();
		} catch (Exception e) {
			lock.lock();
			try {
				treeMap.remove(sn);
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
