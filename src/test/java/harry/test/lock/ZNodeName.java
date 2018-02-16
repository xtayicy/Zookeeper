package harry.test.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class ZNodeName implements Comparable<ZNodeName>{
	private static final Logger logger = LoggerFactory.getLogger(ZNodeName.class);
	private final String name;
	private String prefix;
	private int sequence = -1;
	
	public ZNodeName(String name){
		if(name == null){
			throw new NullPointerException("name musn't be null!");
		}
		
		this.name = name;
		this.prefix = name;
		int idx = name.lastIndexOf('-');
        if (idx >= 0) {
            this.prefix = name.substring(0, idx);
            try {
                this.sequence = Integer.parseInt(name.substring(idx + 1));
                // If an exception occurred we misdetected a sequence suffix,
                // so return -1.
            } catch (NumberFormatException e) {
            	logger.info("Number format exception for " + idx, e);
            } catch (ArrayIndexOutOfBoundsException e) {
            	logger.info("Array out of bounds for " + idx, e);
            }
        }
	}
	
	public String getName(){
		return name;
	}

	@Override
	public int compareTo(ZNodeName o) {
		return 0;
	}
	
	@Override
    public String toString() {
        return name.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ZNodeName sequence = (ZNodeName) o;

        if (!name.equals(sequence.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + 37;
    }
}
