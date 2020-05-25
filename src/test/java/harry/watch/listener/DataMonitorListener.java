package harry.watch.listener;

/**
 * 
 * @author Harry
 *
 */
public interface DataMonitorListener {
	/**
	 * The existence status of the node has changed.
	 */
	void exists(byte data[]);

	/**
	 * The ZooKeeper session is no longer valid.
	 *
	 */
	void closing();
}
