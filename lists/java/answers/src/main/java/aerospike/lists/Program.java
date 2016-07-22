package aerospike.lists;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
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
		System.out.println("***** Counters in Aerospike *****");
		// Constants
		ns = "test"; // Aerospike namespace
		set = "lists"; // Aerospike set name
		String listBin = "list-of-things"; // Aerospike Bin name for a list
		
		// Connecting to Aerospike cluster
		// Specify IP of one of the hosts in the cluster
		String SeedHost = "127.0.0.1";
		// Specify Port that the node is listening on
		int SeedPort = 3000;
		// Establish connection
		client = new AerospikeClient(SeedHost, SeedPort);

		if (client.isConnected()){
			{
				// Make a simple list
				List<Long> aListOfLongs = Arrays.asList(234L, 921L, 877L);
				
				Key key = new Key(ns, set, "a-record-with-a-list");

				// Save a whole list and read the record.
				Bin list = new Bin(listBin, aListOfLongs); 
				client.put(null, key, list);
				// Read the whole list
				Record record = client.get(null, key, listBin);
				
				printRecord(key, record);

				// Add 1 element to the list and print the result
				record = client.operate(null, key, ListOperation.append(listBin, Value.get(99L)), Operation.get(listBin));

				printRecord(key, record);

				// Create policies
				WritePolicy writePolicy = new WritePolicy();
				writePolicy.sendKey = true;
				QueryPolicy queryPolicy = new QueryPolicy();
				
				// Create index on list bin, if it does not exist
				createIndex("listBinIndex", listBin, IndexType.NUMERIC);
				
				// Create many records with values in a list
				queryPolicy.sendKey = true;
				Random rand = new Random(300);
				for (int i = 0; i < 100; i++){
					Key newKey = new Key(ns, set, "a-record-with-a-list-"+i);
					List<Long> aList = new ArrayList<Long>();
					for ( int j = 0; j < 100; j++){
						Long newInt = rand.nextInt(200) + 250L;
						aList.add(newInt);
					}
					client.put(writePolicy, newKey, new Bin(listBin, aList));
				}
				
				// Query the records with list values between 300 and 350
				Statement stmt = new Statement();
				stmt.setNamespace(ns);
				stmt.setSetName(set);
				stmt.setFilters(Filter.range(listBin, IndexCollectionType.LIST, 300, 350));
				
				RecordSet recordSet = client.query(queryPolicy, stmt);
				try {
					while (recordSet.next()){
						printRecord(recordSet.getKey(), recordSet.getRecord());
					}
				} finally {
					recordSet.close();
				}
				
			}
		}	
	}

	public void createIndex(String indexName, String binName, IndexType indexType){
		// check to see if the index exists
		Node node = client.getNodes()[0];
		String result = Info.request(node, "sindex/"+ns);
		boolean indexExists = result.contains(indexName) && result.contains(binName);
		
		// create index
		if (!indexExists){
			IndexTask task = client.createIndex(null, ns, set, indexName, binName, indexType, IndexCollectionType.LIST);
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