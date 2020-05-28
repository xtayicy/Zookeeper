package harry.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author harry
 *
 */
public class ZookeeperClusterTest {
	private static final String HOST = "localhost";
	private static ZkClient zkClient = new ZkClient(ZookeeperConfig.CONNECT_STRING);
	private static final String PATH = "/server";
	private static List<String> hosts = new ArrayList<String>(3);
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperClusterTest.class);
	
	private void monitor(String host,int port){
		if(!zkClient.exists(PATH)){
			zkClient.createPersistent(PATH);
		}
		
		String nodePath = PATH + "/" + host + ":" + port;
		if(!zkClient.exists(nodePath)){
			zkClient.createEphemeral(nodePath);
		}
	}
	
	private void process(int port,String msg) throws IOException{
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(HOST, port));
		serverSocketChannel.configureBlocking(false);
		Selector sel = Selector.open();
		serverSocketChannel.register(sel , SelectionKey.OP_ACCEPT);
		monitor(HOST,port);
		while(true){
			int num = sel.select();
			if(num == 0)
				continue;
			Iterator<SelectionKey> iterator = sel.selectedKeys().iterator();
			while(iterator.hasNext()){
				SelectionKey selectionKey = iterator.next();
				if(selectionKey.isAcceptable()){
					serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
					SocketChannel socketChannel = serverSocketChannel.accept();
					socketChannel.configureBlocking(false);
					socketChannel.register(sel, SelectionKey.OP_READ);
				}else if(selectionKey.isReadable()){
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					ByteBuffer dst = ByteBuffer.allocate(1024);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while((socketChannel.read(dst)) != -1){
						dst.flip();
						while(dst.hasRemaining()){
							baos.write(dst.get());
						}
						
						dst.clear();
					}
					
					socketChannel.register(sel, SelectionKey.OP_WRITE, baos);
				}else if(selectionKey.isWritable()){
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					ByteBuffer src = ByteBuffer.allocate(msg.getBytes().length);
					src.put(msg.getBytes());
					src.flip();
					socketChannel.write(src );
					socketChannel.close();
				}
				
				iterator.remove();
			}
			
		}
	}
	
	@Test
	public void server_7777() throws IOException{
		process(7777, "hello,client!I'm server_7777.");
	}
	
	@Test
	public void server_8888() throws IOException{
		process(8888, "hello,client!I'm server_8888.");
	}

	@Test
	public void server_9999() throws IOException {
		process(9999, "hello,client!I'm server_9999.");
	}

	@Test
	public void client() throws IOException {
		zkClient.unsubscribeChildChanges(PATH, new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				hosts = currentChilds;
			}
		});
		
		hosts = zkClient.getChildren(PATH);
		LOGGER.info(Arrays.toString(hosts.toArray()));
		if(hosts.size() != 0){
			String node = hosts.get(new Random().nextInt(hosts.size()));
			String host = node.split(":")[0];
			int port = Integer.parseInt(node.split(":")[1]);
			communite(host,port);
		}
	}

	private void communite(String host,Integer port) throws IOException {
		SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
		String msg = "hello server,i'm from client!";
		ByteBuffer buffer = ByteBuffer.allocate(msg.getBytes().length);
		buffer.put(msg.getBytes());
		buffer.flip();
		socketChannel.write(buffer);
		socketChannel.shutdownOutput();

		ByteBuffer dst = ByteBuffer.allocate(1024);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while ((socketChannel.read(dst)) != -1) {
			dst.flip();
			while (dst.hasRemaining()) {
				baos.write(dst.get());
			}

			dst.clear();
		}

		LOGGER.info(baos.toString());
		socketChannel.close();
	}
}
