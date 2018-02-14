/**
 * 
 */

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


/**
 * 远程锁定模板
 * @author fansth
 *
 */
public class RemoteLockTemplate {

	private static final AtomicLong sn = new AtomicLong(0);
	
	private final TreeMap<Long, RemoteLockFuture<?>> callbacks = new TreeMap<>();
	
	private final Lock localLock = new ReentrantLock();
	
	private RemoteLock remoteLock;
	
	private int batchSize = 10;
	
	/**
	 * @param zookeeper
	 * @param lockPath 锁定的目录地址 (其实也是也一个域的概念, 如:/tade/kf)
	 * @param batchSize 每次获取锁时批量执行的任务数
	 */
	public RemoteLockTemplate(ZooKeeper zookeeper, String lockPath, int batchSize){
		
		remoteLock = new RemoteLock(zookeeper, lockPath, null);
		
		//注册lock监听器 如果lock获取锁那么执行任务
		remoteLock.setLockListener(new LockListener() {
			
			@Override
			public void lockReleased() {
			}
			
			@SuppressWarnings("unchecked")
			@Override
			public void lockAcquired() {
				try {
					
					List<RemoteLockFuture<?>> futures = new ArrayList<>();
					
					localLock.lock();
					try {
						int size = Math.min(callbacks.size(), RemoteLockTemplate.this.batchSize);
						//好不容获取到锁一次批量执行任务
						for(int i = 0; i < size; i++){
							Entry<Long, RemoteLockFuture<?>> first = callbacks.pollFirstEntry();
							if(first != null){
								RemoteLockFuture<?> future = first.getValue();
								if(!future.isCancelled()){
									futures.add(future);
								}
							}
						}
					} finally {
						localLock.unlock();
					}
					
					if(!futures.isEmpty()){
						for(@SuppressWarnings("rawtypes") RemoteLockFuture future : futures){
							try {
								future.doneWithResult(future.getCallable().call());
							} catch(Exception e){
								future.doneWithException(e);
							}
						}
					}
				} finally {
					remoteLock.unlock();
				}
			}
		});
		
	}
	
	
	/**
	 * 锁定并执行任务
	 * @param callable
	 * @return
	 * @throws RemoteLockUnreachableException
	 */
	public <V> RemoteLockFuture<V> lockAndExecute(Callable<V> callable) throws RemoteLockUnreachableException{
		
		RemoteLockFuture<V> future = null;
		long newSn = -1;
		
		localLock.lock();
		try {
			newSn = sn.incrementAndGet();
			future = new RemoteLockFuture<>(newSn, callbacks, callable);
			callbacks.put(newSn, future);
		} finally {
			localLock.unlock();
		}
		
		try {
			remoteLock.lock();
		} catch (Exception e) {
			
			//遇到异常移除调用
			localLock.lock();
			try {
				callbacks.remove(newSn);
			} finally {
				localLock.unlock();
			}
			
			throw new RemoteLockUnreachableException(e);
		}
		
		return future;
	}
	
	public static void main(String[] args) throws IOException, RemoteLockUnreachableException, InterruptedException, ExecutionException {
		ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 10000, null); 
		RemoteLockTemplate template = new RemoteLockTemplate(zk, "/locks", 20);
		RemoteLockFuture<Object> future = template.lockAndExecute(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				System.out.println("开始sleep");
				Thread.sleep(5000);
				System.out.println("结束sleep");
				return new Date();
			}
		});
		
		System.out.println(future.get());
		
		System.in.read();
		
		System.out.println("over");
	}
		
	
}
