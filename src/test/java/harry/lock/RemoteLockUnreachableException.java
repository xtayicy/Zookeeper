package harry.lock;

/**
 * 
 * @author harry
 *
 */
public class RemoteLockUnreachableException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1587410337124231885L;

	public RemoteLockUnreachableException(Exception e){
		super("Can't reach.",e);
	}
}
