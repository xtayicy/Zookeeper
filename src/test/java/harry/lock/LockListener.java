package harry.lock;

/**
 * 
 * @author harry
 *
 */
public interface LockListener {
	public void acquireLock();
	
	public void releaseLock();
}
