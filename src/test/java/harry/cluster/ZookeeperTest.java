package harry.cluster;

import java.io.IOException;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class ZookeeperTest {
	private ZkClient zkClient = new ZkClient("localhost:2181");
	private static final String PATH = "/zoo";
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperTest.class);
	
	@Test
	public void testCreate(){
		isExist(PATH);
		zkClient.create(PATH, 10, CreateMode.PERSISTENT);
		isExist(PATH);
	}
	
	@Test
	public void testGet(){
		read(PATH);
	}
	
	@Test
	public void testModify(){
		read(PATH);
		zkClient.writeData(PATH, 20);
		read(PATH);
	}
	
	@Test
	public void testDelete(){
		isExist(PATH);
		zkClient.deleteRecursive(PATH);
		isExist(PATH);
	}
	
	@Test
	public void testWather() throws IOException{
		zkClient.subscribeDataChanges(PATH, new IZkDataListener() {
			
			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				
			}
			
			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				System.out.println(dataPath);
				System.out.println(data);
			}
		});
		
		System.in.read();
	}
	
	private void read(String path){
		LOGGER.info(zkClient.readData(path).toString());
	}
	
	private void isExist(String path){
		LOGGER.info(Boolean.toString(zkClient.exists(path)));
	}
}
