package aerospike.maps;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;


/**
 * @author Peter Milne
 */
public class Program {
	private AerospikeClient client;
	private String ns; // Aerospike namespace
	private String set; // Aerospike set name

	public Program()
			throws AerospikeException {
		// Establish a connection to Aerospike cluster
		ClientPolicy cPolicy = new ClientPolicy();
		cPolicy.timeout = 500;
		this.client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);
	}

	public static void main(String[] args) throws AerospikeException {
		try {

			Program as = new Program();

			as.work();

		} catch (Exception e) {
			System.err.println(String.format("Critical error: %s", e.getMessage()));
		}
	}


	public void work() throws Exception {
		System.out.println("***** Maps in Aerospike *****");
		// Constants
		ns = "test"; // Aerospike namespace
		set = "mapss"; // Aerospike set name
		String mapBin = "map-of-things"; // Aerospike Bin name for a list
				
		WritePolicy writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 300; // expire the records in 5 minutes

		
		if (client.isConnected()){
			{
				// Make a simple map and save it and read the record
				Map<String, Object> aMapOfObjects = new HashMap<String, Object>(){{
						put("ersionsnummer", 1);
						put("ieferant", "B");
						put("eisebeginn", "016-10-11");
						put("eiseende", "016-10-11");
						put("bflughafen_Hin", "AH");
						put("nkunfsthafen_Hin", "CN");
						put("luglinie_Hin", "B");
						put("bflugzeit_Hin", 925);
						put("nkunftszeit_Hin", 1720);
						put("bflugzeit_Rueck", 0);
						put("nkunftszeit_Rueck", 0);
						put("lugnummer_Hin", 385);
						put("aehrung", "UR");
						put("reis", 861.0f);
						put("nfant_Preis", 126);
						put("lter_von_1", 2);
						put("lter_bis_1", 11);
						put("reis_Kinderstufe1", 61);
						put("lter_von_2", 0);
						put("lter_bis_2", 0);
						put("otelkategorie", 0);
						put("eisetypkürzel", "F");
						put("eisetyp_Langtext", "ur Flug");
				}};
				
				Key key = new Key(ns, set, "a-record-with-a-map");

				Bin list = new Bin(mapBin, aMapOfObjects); 
				client.put(writePolicy, key, list);
				Record record = client.get(null, key, mapBin);
				
				printRecord(key, record);

				// Add 1 element to the map and print the result

				MapPolicy mapPolicy = new MapPolicy(
						MapOrder.KEY_ORDERED, 
						MapWriteMode.UPDATE_ONLY
						);
				
				client.operate(writePolicy, key, MapOperation.put(mapPolicy, mapBin, Value.get("cat"), Value.get(7)));
				
				record = client.get(null, key, mapBin);
				printRecord(key, record);

				// Add elements to the map and read the whole list.
				
				Map<Value, Value> anotherMap = new HashMap<Value, Value>(){{
					put(Value.get("dogs"), Value.get(1));
					put(Value.get("mice"), Value.get("B"));
				}};
								
				client.operate(writePolicy, key, 
						MapOperation.putItems(mapPolicy, mapBin, anotherMap));
				
				record = client.get(null, key, mapBin);
				printRecord(key, record);		
					
				// Delete a key/value from the map and also return new size of list.
				record = client.operate(writePolicy, key, 
						MapOperation.removeByKey(mapBin, Value.get("dogs"), MapReturnType.KEY),
						MapOperation.size(mapBin));

				printRecord(key, record);	
				
				// Queries on secondary indexes
				
				// Create many records with values in a list
				Random rand = new Random(300);
				for (int i = 0; i < 100; i++){
					Key newKey = new Key(ns, set, "a-record-with-a-map-"+i);
					Map<Value, Value> aMap = new HashMap<Value, Value>();
					for ( int j = 0; j < 100; j++){
						aMap.put(Value.get("dogs"+j), Value.get(i*j));
						aMap.put(Value.get("mice"+j), Value.get(i+j));
					}
					client.put(writePolicy, newKey, new Bin(mapBin, aMap));
				}

				// Create indexes on map bin, if they do not exist
				createIndex("mapKeyIndex", mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
				createIndex("mapValueIndex", mapBin, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
				
				// Query the records with map values between 300 and 350
				
				// Execute the Query
				Statement stmt = new Statement();
				stmt.setNamespace(ns);
				stmt.setSetName(set);
				stmt.setFilters(Filter.range(mapBin, IndexCollectionType.MAPVALUES, 300, 350));
				
				RecordSet recordSet = client.query(null, stmt);
				try {
					System.out.println("\nRecords with map values between 300 and 350:");
					while (recordSet != null & recordSet.next()){
						System.out.println("\t" + recordSet.getKey().userKey);
					}
				} finally {
					if (recordSet != null) recordSet.close();
				}
				
				// Query the records with map values between 300 and 350
				
				// Execute the Query
				stmt = new Statement();
				stmt.setNamespace(ns);
				stmt.setSetName(set);
				stmt.setFilters(Filter.contains(mapBin, IndexCollectionType.MAPKEYS, "dogs7"));
				
				recordSet = client.query(null, stmt);
				try {
					System.out.println("\nRecords with map keys equal to dogs7:");
					while (recordSet != null & recordSet.next()){
						System.out.println("\t" + recordSet.getKey().userKey);
					}
				} finally {
					if (recordSet != null) recordSet.close();
				}
				
				client.close();
			}
		}	
	}

	public void createIndex(String indexName, String binName, IndexType indexType, IndexCollectionType collectionType){
		// check to see if the index exists
		Node node = client.getNodes()[0];
		String result = Info.request(node, "sindex/"+ns);
		boolean indexExists = result.contains(indexName) && result.contains(binName);
		
		// create index
		if (!indexExists){
			IndexTask task = client.createIndex(null, ns, set, indexName, binName, indexType, collectionType);
			task.waitTillComplete();
		}
	}
	
	public void printRecord(Key key, Record record)
	{
		System.out.println("Key");
		if (key == null)
		{
			System.out.println("\tkey == null");
		}
		else 
		{
			System.out.println(String.format("\tNamespace: %s", key.namespace));
			System.out.println(String.format("\t      Set: %s", key.setName));
			System.out.println(String.format("\t      Key: %s", key.userKey));
			System.out.println(String.format("\t   Digest: %s", key.digest.toString()));
		}
		System.out.println("Record");
		if (record == null)
		{
			System.out.println("\trecord == null");
		}
		else
		{
			System.out.println(String.format("\tGeneration: %d", record.generation));
			System.out.println(String.format("\tExpiration: %d", record.expiration));
			System.out.println(String.format("\t       TTL: %d", record.getTimeToLive()));
			System.out.println("Bins");

			for (Map.Entry<String, Object> entry : record.bins.entrySet())
			{
				System.out.println(String.format("\t%s = %s", entry.getKey(), entry.getValue().toString()));
			}
		}
	}

}
