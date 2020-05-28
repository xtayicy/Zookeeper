package harry.lock;

import java.lang.ref.WeakReference;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class RemoteLockFuture<V> implements Future<V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLockFuture.class);
	private final CountDownLatch countDownLatch;
	private V result;
	private final Callable<V> callable;
	private final AtomicLong localSn;
	
	public RemoteLockFuture(Long sn,TreeMap<Long,RemoteLockFuture<?>> treeMap,Callable<V> callable) {
		this.localSn = new AtomicLong(sn);
		countDownLatch = new CountDownLatch(1);
		this.callable = callable;
	}
	
	void doneWithResult(V result){
		this.result = result;
		countDownLatch.countDown();
	}
	
	Callable<?> getCallable(){
		return callable;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		
		return false;
	}

	@Override
	public boolean isCancelled() {
		LOGGER.info("isCancelled...");
		
		return localSn.get() == 0;
	}

	@Override
	public boolean isDone() {
		LOGGER.info("isDone....");
		
		return countDownLatch.getCount() == 0;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		LOGGER.info("get....");
		countDownLatch.await();
		
		return result;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		
		return null;
	}
}
