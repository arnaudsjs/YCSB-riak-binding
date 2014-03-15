package riakBinding.java;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Quora;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.StoreObject;
import com.basho.riak.client.query.BucketMapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import consistencyTests.util.StringToStringMap;

/*
Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Administrative Contact: dnet-project-office@cs.kuleuven.be
Technical Contact: arnaud.schoonjans@student.kuleuven.be
*/
public class RiakClient extends DB {

	public static final int OK = 0;
	public static final int ERROR = -1;
	private static final Quora DEFAULT_READ_QUORUM = Quora.QUORUM;
	private static final Quora DEFAULT_WRITE_QUORUM = Quora.QUORUM;
	private static final Quora DEFAULT_DELETE_QUORA = Quora.QUORUM;
	public static final String WRITE_NODE_PROPERTY = "writenode";
	
	private final int maxConnections = 50;
	private IRiakClient clientForModifications;
	private IRiakClient clientForConsistencyChecks;
	
	public RiakClient(){
		this.clientForModifications = null;
		this.clientForConsistencyChecks = null;
	}
	
	private String[] getIpAddressesOfNodes() throws DBException {
		String hosts = getProperties().getProperty("hosts");
		if (hosts == null)
			throw new DBException("Required property \"hosts\" missing for RiakClient");
		return hosts.split(",");
	}
	
	private HTTPClusterConfig getClusterConfiguration(String[] hosts) throws DBException {
		HTTPClusterConfig clusterConfig = new HTTPClusterConfig(
				this.maxConnections);
		HTTPClientConfig httpClientConfig = HTTPClientConfig.defaults();
		clusterConfig.addHosts(httpClientConfig, hosts);
		return clusterConfig;
	}
	
	private IRiakClient createRiakClient(String[] hosts) throws DBException{
		HTTPClusterConfig clusterConfig = getClusterConfiguration(hosts);
		try {
			return RiakFactory.newClient(clusterConfig);
		} catch (RiakException e) {
			throw new DBException("Unable to connect to cluster nodes");
		}
	}
	
	@Override
	public void init() throws DBException {
		String[] allHosts = this.getIpAddressesOfNodes();
		this.clientForConsistencyChecks = this.createRiakClient(allHosts);
		String writeNode = getProperties().getProperty(WRITE_NODE_PROPERTY);
		if(writeNode == null)
			this.clientForModifications = this.clientForConsistencyChecks;
		else
			this.clientForModifications = this.createRiakClient(new String[]{writeNode});
	}
	
	@Override
	public void cleanup() throws DBException {
		this.shutdownAllClients();
	}
	
	private void shutdownAllClients(){
		this.shutdownClient(this.clientForModifications);
		this.shutdownClient(this.clientForConsistencyChecks);
	}
	
	private void shutdownClient(IRiakClient client){
		if(client != null)
			client.shutdown();
	}
	
	private StringToStringMap executeReadQuery(IRiakClient client, String bucketName, String key) {
		try {
			Bucket bucket = client.fetchBucket(bucketName).execute();
			FetchObject<StringToStringMap> fetchObj = bucket.fetch(key, StringToStringMap.class);
			return fetchObj.r(DEFAULT_READ_QUORUM).execute();
		} catch (Exception exc) {
			System.out.println("EXCEPTION: " + exc);
			return null;
		}
	}
	
	private int executeWriteQuery(IRiakClient client, String bucketName, String key, StringToStringMap dataToWrite){
		try {
			Bucket bucket = client.fetchBucket(bucketName).execute();
			StoreObject<StringToStringMap> storeObject = bucket.store(key, dataToWrite);
			storeObject.w(DEFAULT_WRITE_QUORUM).execute();
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}
	
	private void copyRequestedFieldsToResultMap(Set<String> fields,
			StringToStringMap inputMap,
			HashMap<String, ByteIterator> result) {
		for (String field : fields) {
			ByteIterator value = inputMap.getAsByteIt(field);
			result.put(field, value);
		}
	}

	private void copyAllFieldsToResultMap(StringToStringMap inputMap, 
								Map<String, ByteIterator> result){
		for(String key: inputMap.keySet()){
			ByteIterator value = inputMap.getAsByteIt(key);
			result.put(key, value);
		}
	}
	
	private StringToStringMap executeReadQuery(String bucketName, String key) {
		try {
			Bucket bucket = this.clientForConsistencyChecks.fetchBucket(bucketName).execute();
			FetchObject<StringToStringMap> fetchObj = bucket.fetch(key, StringToStringMap.class);
			StringToStringMap result = fetchObj.r(DEFAULT_READ_QUORUM).execute();
			if(result == null)
				throw new Exception("key not found" + key);
			else
				return result;
		} catch (Exception exc) {
			return null;
		}
	}
	
	@Override
	public int read(String bucketName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		StringToStringMap queryResult = executeReadQuery(bucketName, key);
		if (queryResult == null) {
			return ERROR;
		}
		if (fields != null) {
			this.copyRequestedFieldsToResultMap(fields, queryResult, result);
		} else {
			this.copyAllFieldsToResultMap(queryResult, result);
		}
		return OK;
	}
	
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		BucketMapReduce m = this.clientForConsistencyChecks.mapReduce(table);
		m.addMapPhase(new JSSourceFunction(getMapPhaseFunction(startkey)), false);
		m.addReducePhase(new JSSourceFunction(getReducePhaseFunction(recordcount)), true);
		MapReduceResult mapReduceResult;
		try {
			mapReduceResult = m.execute();
		} catch (Exception e) {
			return ERROR;
		}
		Collection<StringToStringMap> mapredResult = mapReduceResult.getResult(StringToStringMap.class);
		this.putScanResultInResultMap(mapredResult, fields, result);
		return OK;
	}

	private String getMapPhaseFunction(String startKey){
		return "function(riakObject){ " +
				"var numPartCurrentKey = parseInt(riakObject.key.substring(4)); " +
				"var numPartStartKey = parseInt(\"" + startKey + "\".substring(4)); " +
				"var value = riakObject.values[0].data; " +
				"if(numPartCurrentKey >= numPartStartKey){ " +
                	"parsedValue = JSON.parse(value);" +
                	"parsedValue.key = numPartCurrentKey; " +
                	"return [JSON.stringify(parsedValue)]; }" + 
                "else {" + 
                	"return []; }" +
               "} ";
	}
	
	private String getReducePhaseFunction(int amountOfValuesToRetrieve){
		return  "function(keyValuePairs){ " +
				 "var amount = " + amountOfValuesToRetrieve + "; " +
				 "var sortedPairs = keyValuePairs.sort(function(a, b) { parsedA = JSON.parse(a); parsedB = JSON.parse(b); " +
				 														"return (parsedA.key < parsedB.key ? -1 : ( parsedA.key > parsedB.key ? 1 : 0)); }); " +
				 "return sortedPairs.slice(0,amount); " +
                 "} ";
	}
	
	private void putScanResultInResultMap(
			Collection<StringToStringMap> mapredResult, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		for (StringToStringMap currentMap : mapredResult) {
			HashMap<String, ByteIterator> mapToAdd = new HashMap<String, ByteIterator>();
			if (fields == null)
				this.copyAllFieldsToResultMap(currentMap, mapToAdd);
			else
				this.copyRequestedFieldsToResultMap(fields, currentMap,
						mapToAdd);
			result.add(mapToAdd);
		}
	}
	
	@Override
	public int update(String bucketName, String key,
			HashMap<String, ByteIterator> values) {
		StringToStringMap queryResult = this.executeReadQuery(this.clientForModifications, bucketName, key);
		if(queryResult == null)
			return ERROR;
		for(String fieldToUpdate: values.keySet()){
			ByteIterator newValue = values.get(fieldToUpdate);
			queryResult.put(fieldToUpdate, newValue);
		}
		return this.executeWriteQuery(this.clientForModifications, bucketName, key, queryResult);
	}

	@Override
	public int insert(String bucketName, String key,
			HashMap<String, ByteIterator> values) {
		StringToStringMap dataToInsert = new StringToStringMap(values);
		return this.executeWriteQuery(this.clientForModifications, bucketName, key, dataToInsert);
	}
	
	@Override
	public int delete(String bucketName, String key) {
		try {
			Bucket bucket = this.clientForModifications.fetchBucket(bucketName).execute();
			DeleteObject delObj = bucket.delete(key);
			delObj.rw(DEFAULT_DELETE_QUORA).execute();
		} catch (RiakRetryFailedException e) {
			return ERROR;
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}

}