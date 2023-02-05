package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import testing.TestingVars;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", TestingVars.port);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}



	@Test
	public void testNullKey(){
		String value = "validValue";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(null);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);

		try {
			response = kvClient.put(null, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testNullValue(){
		String key = "validKey";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, null);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testBlankValueDelete(){
		String key = "blankKey";
		String value = "validValue";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testBlankKey(){
		String value = "validValue";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put("", value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testKeySize() {
		int keySize = 10000;
		String value = "validValue";
		char[] keyArr = new char[keySize];
		for (int i =0; i<keySize; i++){
			keyArr[i] = '0';
		}
		String largeKey = new String(keyArr);

		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(largeKey);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);


		try {
			response = kvClient.put(largeKey, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testValueSize() {
		int valueSize = 100000;
		String key = "validKey";
		char[] valueArr = new char[valueSize];
		for (int i =0; i<valueSize; i++){
			valueArr[i] = '0';
		}
		String largeValue = new String(valueArr);

		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(key, largeValue);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testUnsetDelete() {
		String key = "doesNotExist";
		
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);

		try {
			response = kvClient.put(key, "");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testGetMultiClient() {
		int numClients = 10;
		KVStore[] kvClientArr = new KVStore[numClients];
		
		IKVMessage response = null;
		Exception ex = null;

		String key = "foo";
		String value = "bar";

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		

		for(int i =0; i<numClients; i++){
			kvClientArr[i] = new KVStore("localhost", TestingVars.port);
			try {
				kvClientArr[i].connect();
			} catch (Exception e) {
				ex = e;
			}
			assert(ex == null);
		}

		for(int i = 0; i<numClients; i++){
			try {
				response = kvClientArr[i].get(key);
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getValue().equals("bar"));
			assertTrue(response.getStatus() == StatusType.GET_SUCCESS );
		}

		for(int i =0; i<numClients; i++){
			kvClientArr[i].disconnect();
		}
	}


	@Test
	public void testPutMultiClient() {
		int numClients = 10;
		KVStore[] kvClientArr = new KVStore[numClients];
		
		IKVMessage response = null;
		Exception ex = null;

		

		for(int i =0; i<numClients; i++){
			kvClientArr[i] = new KVStore("localhost", TestingVars.port);
			try {
				kvClientArr[i].connect();
			} catch (Exception e) {
				ex = e;
			}
			assert(ex == null);
		}

		for(int i = 0; i<numClients; i++){
			try {
				response = kvClientArr[i].put(Integer.toString(i), Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getValue().equals(Integer.toString(i)));
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS );
		
		}

		for(int i =0; i<numClients; i++){
			kvClientArr[i].disconnect();
		}
	}

	@Test
	public void testUpdateMultiClient() {
		int numClients = 10;
		KVStore[] kvClientArr = new KVStore[numClients];
		String key = "foo";
		
		IKVMessage response = null;
		Exception ex = null;

		

		for(int i =0; i<numClients; i++){
			kvClientArr[i] = new KVStore("localhost", TestingVars.port);
			try {
				kvClientArr[i].connect();
			} catch (Exception e) {
				ex = e;
			}
			assert(ex == null);
		}

		for(int i = 0; i<numClients; i++){
			try {
				response = kvClientArr[i].put(key, Integer.toString(i));
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getValue().equals(Integer.toString(i)));
			assertTrue(response.getStatus() == StatusType.PUT_UPDATE );
		}

		for(int i =0; i<numClients; i++){
			kvClientArr[i].disconnect();
		}
	}

	@Test
	public void testDeleteMultiClient() {
		int numClients = 10;
		KVStore[] kvClientArr = new KVStore[numClients];
		String key = "toBeDeleted";
		
		IKVMessage response = null;
		Exception ex = null;

		

		for(int i =0; i<numClients; i++){
			kvClientArr[i] = new KVStore("localhost", TestingVars.port);
			try {
				kvClientArr[i].connect();
			} catch (Exception e) {
				ex = e;
			}
			assert(ex == null);
		}

		for(int i = 0; i<numClients; i++){
			try {
				response = kvClientArr[i].put(key, Integer.toString(i));
				response = kvClientArr[i].put(key, "null");
			} catch (Exception e) {
				ex = e;
			}
			assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		}

		for(int i =0; i<numClients; i++){
			kvClientArr[i].disconnect();
		}
	}

}
