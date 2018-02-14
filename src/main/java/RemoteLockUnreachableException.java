/**
 * 
 */

/**
 * @author fansth
 *
 */
public class RemoteLockUnreachableException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8990251773838028850L;

	public RemoteLockUnreachableException(Throwable cause){
		super("远程锁处于不可达", cause);
	}
	
}
