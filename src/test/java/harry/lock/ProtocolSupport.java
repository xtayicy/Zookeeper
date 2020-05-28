package harry.lock;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class ProtocolSupport {
	private static final Logger LOG = LoggerFactory.getLogger(ProtocolSupport.class);
	private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
	protected final ZooKeeper zooKeeper;
	private AtomicBoolean closed = new AtomicBoolean(false);
	private int retryTimes = 10;
	private long retryDelay = 500L;
	
	public ProtocolSupport(ZooKeeper zooKeeper){
		this.zooKeeper = zooKeeper;
	}
	
	public boolean isClosed(){
		return closed.get();
	}
	
	protected void ensurePathExists(String path) throws KeeperException, InterruptedException{
		ensureExists(path,null,acl,CreateMode.PERSISTENT);
	}
	
	protected void ensureExists(String path,byte[] data,List<ACL> acl,CreateMode flags) throws KeeperException, InterruptedException{
		retryOperation(new ZookeeperOperation() {
			
			@Override
			public boolean execute() throws KeeperException, InterruptedException {
				Stat exists = zooKeeper.exists(path, null);
				LOG.info("exists :" + exists);
				if(exists != null){
					return true;
				}
				
				zooKeeper.create(path, data, acl, flags);
				return true;
			}
		});
	}
	
	protected Object retryOperation(ZookeeperOperation zookeeperOperation) throws KeeperException, InterruptedException{
		KeeperException exception = null;
		for (int i = 0; i < retryTimes; i++) {
			try {
				return zookeeperOperation.execute();
			}  catch (KeeperException.SessionExpiredException e) {
                LOG.warn("Session expired for: " + zooKeeper + " so reconnecting due to: " + e, e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt " + i + " failed with connection loss so " +
                		"attempting to reconnect: " + e, e);
                retryDelay(i);
            }
		}
		
		throw exception;
	}
	
	protected void retryDelay(int attemptCount){
		if(attemptCount > 0){
			try {
				Thread.sleep(attemptCount * retryDelay);
			} catch (InterruptedException e) {
				LOG.debug("Failed to sleep: " + e, e);
			}
		}
	}

	public List<ACL> getAcl() {
		return acl;
	}

	public void setAcl(List<ACL> acl) {
		this.acl = acl;
	}

	public long getRetryDelay() {
		return retryDelay;
	}

	public void setRetryDelay(long retryDelay) {
		this.retryDelay = retryDelay;
	}
}
