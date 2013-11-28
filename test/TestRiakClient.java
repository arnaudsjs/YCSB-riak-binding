package riakBinding.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import riakBinding.java.RiakClient;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class TestRiakClient {

	private static RiakClient client;
	
	@BeforeClass
	public static void initializerClient() throws RiakException {
		String[] hosts = {"127.0.0.1:2222,127.0.0.1:3333"};
		HTTPClusterConfig clusterConfig = new HTTPClusterConfig(50);
		HTTPClientConfig httpClientConfig = HTTPClientConfig.defaults();
		clusterConfig.addHosts(httpClientConfig, hosts);
		IRiakClient riakClient = RiakFactory.newClient(clusterConfig);
		client = new RiakClient(riakClient);
	}
	
	@Test
	public void test(){
		// Initialize parameters
		String bucketName = "abucket";
		String keyInBucket = "aKey";
		String mapKey = "mapkey";
		String mapValue = "mapValue";
		// Insert operation
		HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
		map.put(mapKey, new StringByteIterator(mapValue));
		client.insert(bucketName, keyInBucket, map);
		// Read operation
		HashMap<String, ByteIterator> readMap = new HashMap<String, ByteIterator>();
		int success = client.read(bucketName, keyInBucket, null, readMap);
		// Assert reads
		assertTrue(success == 0);
		assertTrue(readMap.size() == 1);
		ByteIterator byteIterator = readMap.get(mapKey);
		assertEquals(byteIterator.toString(), mapValue);
		// Delete operation
		success = client.delete(bucketName, keyInBucket);
		// Assert deletion
		assertTrue(success == 0);
		success = client.read(bucketName, keyInBucket, null, new HashMap<String, ByteIterator>());
		assertTrue(success == -1);
	}
	
}
