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
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

/**
 * 
 * @author harry
 *
 */
public class ZookeeperClusterTest {
	private static final String HOST = "localhost";
	private static final ZkClient zkClient = new ZkClient("localhost:2181");
	private static final String PATH = "/server";
	private static List<String> hosts = new ArrayList<String>(3);
	
	private void monitor(String host,int port){
		if(!zkClient.exists(PATH)){
			zkClient.createPersistent(PATH);
		}
		
		String nodePath = PATH + "/" + host + ":" + port;
		if(!zkClient.exists(nodePath)){
			zkClient.createEphemeral(nodePath);
		}
	}
	
	@Test
	public void server_7777() throws IOException{
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		final int port = 7777;
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
					int len;
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while((len = socketChannel.read(dst)) != -1){
						dst.flip();
						while(dst.hasRemaining()){
							baos.write(dst.get());
						}
						
						dst.clear();
					}
					
					socketChannel.register(sel, SelectionKey.OP_WRITE, baos);
				}else if(selectionKey.isWritable()){
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					String msg = "hellp,client!I'm server_7777.";
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
	public void server_8888() throws IOException{
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		final int port = 8888;
		serverSocketChannel.bind(new InetSocketAddress(HOST, port));
		serverSocketChannel.configureBlocking(false);
		
		Selector sel = Selector.open();
		serverSocketChannel.register(sel , SelectionKey.OP_ACCEPT);
		monitor(HOST, port);
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
					int len;
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while((len = socketChannel.read(dst)) != -1){
						dst.flip();
						while(dst.hasRemaining()){
							baos.write(dst.get());
						}
						
						dst.clear();
					}
					
					socketChannel.register(sel, SelectionKey.OP_WRITE,baos);
				}else if(selectionKey.isWritable()){
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					String msg = "hello,client!I'm server_8888";
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
	public void server_9999() throws IOException {
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		int port = 9999;
		serverSocketChannel.bind(new InetSocketAddress(HOST, port));
		serverSocketChannel.configureBlocking(false);

		Selector selector = Selector.open();
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		monitor(HOST, port);
		while (true) {
			int num = selector.select();
			if (num == 0)
				continue;
			Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
			while (iterator.hasNext()) {
				SelectionKey selectionKey = iterator.next();
				if (selectionKey.isAcceptable()) {
					serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
					SocketChannel socketChannel = serverSocketChannel.accept();
					socketChannel.configureBlocking(false);
					socketChannel.register(selector, SelectionKey.OP_READ);
				} else if (selectionKey.isReadable()) {
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					int len;
					ByteBuffer dst = ByteBuffer.allocate(1024);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while ((len = socketChannel.read(dst)) != -1) {
						dst.flip();
						while (dst.hasRemaining()) {
							baos.write(dst.get());
						}
						dst.clear();
					}

					socketChannel.register(selector, SelectionKey.OP_WRITE, baos);
				} else if (selectionKey.isWritable()) {
					SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
					String msg = "hello,client!I'm server_9999.";
					ByteBuffer src = ByteBuffer.allocate(msg.getBytes().length);
					src.put(msg.getBytes());
					src.flip();
					socketChannel.write(src);
					socketChannel.close();
				}

				iterator.remove();
			}
		}
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
		System.out.println(hosts);
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

		int len;
		ByteBuffer dst = ByteBuffer.allocate(1024);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while ((len = socketChannel.read(dst)) != -1) {
			dst.flip();
			while (dst.hasRemaining()) {
				baos.write(dst.get());
			}

			dst.clear();
		}

		System.out.println(baos);
		socketChannel.close();
	}
}
