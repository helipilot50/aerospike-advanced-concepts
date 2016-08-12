package aerospike.counters;


import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author Peter Milne
 */
public class CountersExercises {
	private AerospikeClient client;

	public CountersExercises()
			throws AerospikeException {
		// Establish a connection to Aerospike cluster
		ClientPolicy cPolicy = new ClientPolicy();
		cPolicy.timeout = 500;
		this.client = new AerospikeClient(cPolicy, "10.211.55.101", 3000);
	}

	public static void main(String[] args) throws AerospikeException {
		try {

			CountersExercises as = new CountersExercises();

			as.work();

		} catch (Exception e) {
			System.err.println(String.format("Critical error: %s", e.getMessage()));
		}
	}


	public void work() throws Exception {
		System.out.println("***** Counters in Aerospike *****");
		// Constants
		final String ns = "test"; // Aerospike namespace
		final String set = "counters"; // Aerospike set name
		final String catCountBin = "cat-counter"; // Aerospike Bin name for a "cat" counter
		final String dogCountBin = "dog-counter"; // Aerospike Bin name for a "dog" counter

		// Connecting to Aerospike cluster
		// Specify IP of one of the hosts in the cluster
		String SeedHost = "10.211.55.101";
		// Specify Port that the node is listening on
		int SeedPort = 3000;
		// Establish connection
		AerospikeClient client = new AerospikeClient(SeedHost, SeedPort);

		if (client.isConnected()){
			{
				// Add integer to the cat counter, and read the record.
				Key key = new Key(ns, set, "a-record-with-one-counter");

				// TODO Increment cat counter by 1
				Record record = null;

				printRecord(key, record);

				// Add integer to the cat counter and dog counter, and read the record.
				key = new Key(ns, set, "a-record-with-two-counters");

				// TODO Increment cat counter by 3
				// TODO Increment dog counter by 2
				record = null;

				printRecord(key, record);

				// Subtract integer from the cat counter , and read the record.
				// TODO Decrement cat counter by 1
				record = null;

				printRecord(key, record);
			}
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