package harry.watch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import harry.watch.listener.DataMonitorListener;
import harry.watch.monitor.DataMonitor;

/**
 * 
 * @author Harry
 *
 */
public class Executor implements Runnable, Watcher, DataMonitorListener {
	private String fileName;
	private ZooKeeper zooKeeper;
	private DataMonitor dataMonitor;

	public Executor(String hostPort, String znode, String fileName) throws IOException {
		this.fileName = fileName;
		this.zooKeeper = new ZooKeeper(hostPort, 3000, this);
		this.dataMonitor = new DataMonitor(zooKeeper, znode, this);
	}

	public static void main(String[] args) throws IOException {
		String hostPort = "127.0.0.1:2181";
		String znode = "/test";
		String fileName = "data.txt";

		new Executor(hostPort, znode, fileName).run();
	}

	@Override
	public void run() {
		try {
			synchronized (this) {
				while (!dataMonitor.isDead()) {
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		dataMonitor.process(event);
	}

	@Override
	public void exists(byte[] data) {
		if(data != null){
			try (FileOutputStream fos = new FileOutputStream(fileName);) {
				fos.write(data);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}else{
			File file = new File(fileName);
			if(file.exists()){
				file.delete();
			}
		}
	}

	@Override
	public void closing() {
		synchronized (this) {
			notifyAll();
		}
	}
}
