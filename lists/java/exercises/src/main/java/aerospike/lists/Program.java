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
		System.out.println("***** Lists in Aerospike *****");
		// Constants
		ns = "test"; // Aerospike namespace
		set = "lists"; // Aerospike set name
		String listBin = "list-of-things"; // Aerospike Bin name for a list
		
		WritePolicy writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 300; // expire the records in 5 minutes

		
		if (client.isConnected()){
			{
				// Make a simple list and save it and read the record
				List<Long> aListOfLongs = Arrays.asList(234L, 921L, 877L);
				
				Key key = new Key(ns, set, "a-record-with-a-list");

				// TODO save a List to Aerospike
				
				Record record = client.get(null, key, listBin);
				
				printRecord(key, record);

				// Add 1 element to the list and print the result
				
				// TODO append 99 to the list
				
				record = client.get(null, key, listBin);
				printRecord(key, record);

				// Add values to the list and read the whole list.
				List<Value> inputList = new ArrayList<Value>();
				inputList.add(Value.get(55));
				inputList.add(Value.get(77));
				
				// TODO append inputList to the list already saved
				
				record = client.get(null, key, listBin);
				printRecord(key, record);		
					
				// Pop value from end of list and also return new size of list.
				
				// TODO pop the last element in the list, return it and the new size od the list

				printRecord(key, record);	
				
				// Query the records with list values between 300 and 350
				
				// Create index on list bin, if it does not exist
				createIndex("listBinIndex", listBin, IndexType.NUMERIC);
				
				// Create many records with values in a list
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
				
				// Execute the Query
				Statement stmt = new Statement();
				stmt.setNamespace(ns);
				stmt.setSetName(set);
				
				// TODO set a filter to perform a range query on the values in the list
				// TODO Query using the statement
				
				RecordSet recordSet = null;
				try {
					System.out.println("\nRecords with values between 300 and 350:");
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

	public void createIndex(String indexName, String binName, IndexType indexType){
		// check to see if the index exists
		Node node = client.getNodes()[0];
		String result = Info.request(node, "sindex/"+ns);
		boolean indexExists = result.contains(indexName) && 
				result.contains(set) &&
				result.contains(binName);
		
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