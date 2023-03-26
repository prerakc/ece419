package testing;

import java.net.UnknownHostException;

import app_kvServer.KVServer;
import client.KVStore;

import junit.framework.TestCase;
import org.junit.Test;


public class NonDistributedKVStoreTest extends TestCase {

	private static String address = "localhost";
	private static int port = 50069;
	private static int cacheSize = 0;
	private static String strategy = "foo";
	private static String dataDir = "./data/test";
	private static String dataProps = "non_distributed_kvstore_test.properties";

	static {
		new KVServer(address, port, cacheSize, strategy, dataDir, dataProps).start();
	}

	@Test
	public void testConnectionSuccess() {
		Exception ex = null;
		
		KVStore kvClient = new KVStore(address, port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);

		kvClient.disconnect();
	}

	@Test
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", port);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}

	@Test
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore(address, 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
}
