package testing;

import app_kvServer.KVServer;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import testing.TestingVars;


public class InteractionTest extends TestCase {

	private static KVServer kvServer;

	private static String address = "localhost";
	private static int port = 50069;
	private static int cacheSize = 0;
	private static String strategy = "foo";
	private static String dataDir = "./data/test";
	private static String dataProps = "connection_test.properties";

	private KVStore kvClient;

	public void setUp() {
		kvClient = new KVStore(address, port);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}

	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
			System.out.println(response.getStatus().toString());
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			System.out.println(response.getStatus().toString());

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			System.out.println(response.getStatus().toString());

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
				System.out.println("here " + response.getStatus().toString() + String.format(" %s %s", response.getKey().toString(), response.getValue().toString()));
			} catch (Exception e) {
				ex = e;
			}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

//	@Test
//	public void testGetUnsetValue() {
//		String key = "an unset value";
//		IKVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.get(key);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
//	}
}
