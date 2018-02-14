/**
 * 
 */

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 远程锁future
 * @author fansth
 *
 */
public class RemoteLockFuture<V> implements Future<V> {

	private final AtomicLong lockSn;
	
	private final WeakReference<Map<Long, RemoteLockFuture<?>>> ref;
	
	private V result;
	
	private Exception exception;
	
	private final CountDownLatch countdown;
	
	private final Callable<V> callable;
	
	RemoteLockFuture(long lockSn, Map<Long, RemoteLockFuture<?>> futures, Callable<V> callable) {
		super();
		this.lockSn = new AtomicLong(lockSn);
		this.ref = new WeakReference<>(futures);
		this.countdown = new CountDownLatch(1);
		this.callable = callable;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		long sn = lockSn.get();
		if(sn != 0){
			if(lockSn.compareAndSet(sn, 0)){
				Map<Long, RemoteLockFuture<?>> callables = this.ref.get();
				if(callables != null){
					callables.remove(sn);
				}
				return true;
			}
		}
		return false;
	}
	
	void doneWithResult(V result){
		this.result = result;
		countdown.countDown();
	}
	
	void doneWithException(Exception e){
		this.exception = e;
		countdown.countDown();
	}
	
	Callable<?> getCallable(){
		return callable;
	}

	@Override
	public boolean isCancelled() {
		return lockSn.get() == 0;
	}

	@Override
	public boolean isDone() {
		return countdown.getCount() == 0;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		countdown.await();
		if(exception != null){
			throw new ExecutionException(exception);
		}
		return result;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException,ExecutionException, TimeoutException {
		if(!countdown.await(timeout, unit)){
			throw new TimeoutException("获取远程锁超时!");
		}
		if(exception != null){
			throw new ExecutionException(exception);
		}
		return result;
	}

}
