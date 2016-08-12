using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace AerospikeCounters
{
	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("***** Counters in Aerospike *****");

			// Constants
			const string ns = "test"; // Aerospike namespace
			const string set = "counters"; // Aerospike set name
			const string CatCountBin = "cat-counter"; // Aerospike Bin name for a "cat" counter
			const string DogCountBin = "dog-counter"; // Aerospike Bin name for a "dog" counter

			// Connecting to Aerospike cluster
			// Specify IP of one of the hosts in the cluster
			string SeedHost = "10.211.55.101";
			// Specity Port that the node is listening on
			int SeedPort = 3000;
			// Establish connection
			AerospikeClient client = new AerospikeClient(SeedHost, SeedPort);

			if (client.Connected)
			{
				// Add integer to the cat counter, and read the record.
				Key key = new Key(ns, set, "a-record-with-one-counter");

				// TODO Increment cat counter by 1
				Record record = null;

				PrintRecord(key, record);

				// Add integer to the cat counter and dog counter, and read the record.
				key = new Key(ns, set, "a-record-with-two-counters");

				// TODO Increment cat counter by 3
				// TODO Increment dog counter by 2
				record = null;

				PrintRecord(key, record);

				// Subtract integer from the cat counter , and read the record.
				// TODO Decrement cat counter by 1
				record = null;

				PrintRecord(key, record);

				// close Aerospike at the end of your program
				client.Close();
			}
		}
		public static void PrintRecord(Key key, Record record)
		{
			Console.WriteLine("Key");
			if (key == null)
			{
				Console.WriteLine("\tkey == null");
			}
			else 
			{
				Console.WriteLine(String.Format("\tNamespace: {0}", key.ns));
				Console.WriteLine(String.Format("\t      Set: {0}", key.setName));
				Console.WriteLine(String.Format("\t      Key: {0}", key.userKey));
				Console.WriteLine(String.Format("\t   Digest: {0}", key.digest));
			}
			Console.WriteLine("Record");
			if (record == null)
			{
				Console.WriteLine("\trecord == null");
			}
			else
			{
				Console.WriteLine(String.Format("\tGeneration: {0}", record.generation));
				Console.WriteLine(String.Format("\tExpiration: {0}", record.expiration));
				Console.WriteLine(String.Format("\t       TTL: {0}", record.TimeToLive));
				Console.WriteLine("Bins");

				foreach (KeyValuePair<string, Object> entry in record.bins)
				{
					Console.WriteLine(String.Format("\t{0} = {1}", entry.Key, entry.Value));
				}
			}
		}
	}
}
