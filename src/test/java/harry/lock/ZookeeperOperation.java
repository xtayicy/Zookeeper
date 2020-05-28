package harry.lock;

import org.apache.zookeeper.KeeperException;

/**
 * 
 * @author harry
 *
 */
public interface ZookeeperOperation{
	public boolean execute() throws KeeperException, InterruptedException;
}
