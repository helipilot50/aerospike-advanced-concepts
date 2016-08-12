using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace AerospikeLists
{
	public class Program
	{

		private string ns = "test"; // Aerospike namespace
		private string set = "lists"; // Aerospike set name
		private string listBin = "list-of-things"; // Aerospike Bin name for a list
		private AerospikeClient client;

		public static void Main(string[] args)
		{
			try
			{
				Program prog = new Program();
				prog.Work();

			}
			catch (Exception e)
			{
				Console.WriteLine("Critical error: {0}", e.Message);
			}
		}

		public Program()
		{
			// Establish a connection to Aerospike cluster
			ClientPolicy cPolicy = new ClientPolicy();
			cPolicy.timeout = 500;
			client = new AerospikeClient(cPolicy, "10.211.55.101", 3000);

		}

		public void Work()
		{
			Console.WriteLine("***** Lists in Aerospike *****");

			WritePolicy writePolicy = new WritePolicy(); // Create a WritePolicy
			writePolicy.sendKey = true; // Save the Key on each write
			writePolicy.expiration = 300; // expire the records in 5 minutes

			if (client.Connected)
			{
				// Make a simple list and save it and read the record
				List<long> aListOfLongs = new List<long>()
				{
					234L, 921L, 877L
				};

				Key key = new Key(ns, set, "a-record-with-a-list");

				// TODO save a List to Aerospike

				Record record = client.Get(null, key, listBin);

				PrintRecord(key, record);

				// Add 1 element to the list and print the result

				// TODO append 99 to the list

				record = client.Get(null, key, listBin);
				PrintRecord(key, record);

				// Add values to the list and read the whole list.
				List<Value> inputList = new List<Value>()
				{
					Value.Get(55),
					Value.Get(77)
				};

				// TODO append the inputList to the list

				record = client.Get(null, key, listBin);
				PrintRecord(key, record);

				// Pop value from end of list and also return new size of list.

				// TODO pop the last element in the list, return it and the new size of the list

				PrintRecord(key, record);

				// Query the records with list values between 300 and 350

				// Create index on list bin, if it does not exist
				CreateIndex("listBinIndex", listBin, IndexType.NUMERIC);

				// Create many records with values in a list
				Random rand = new Random(300);
				for (int i = 0; i < 100; i++)
				{
					Key newKey = new Key(ns, set, "a-record-with-a-list-" + i);
					List<long> aList = new List<long>();
					for (int j = 0; j < 100; j++)
					{
						long newInt = rand.Next(250, 450);
						aList.Add(newInt);
					}
					client.Put(writePolicy, newKey, new Bin(listBin, aList));
				}

				// Execute the Query
				Statement stmt = new Statement();
				stmt.Namespace = ns;
				stmt.SetName = set;

				// TODO set a filter to perform a range query on the values between 300 and 350
				// TODO Query using the statement

				RecordSet recordSet = null;
				try
				{
					Console.WriteLine("\nRecords with values between 300 and 350:");
					while (recordSet != null & recordSet.Next())
					{
						Console.WriteLine("\t" + recordSet.Key.userKey);
					}
				}
				finally
				{
					if (recordSet != null) recordSet.Close();
				}

				// close Aerospike at the end of your program
				client.Close();
			}
		}

		public void CreateIndex(String indexName, String binName, IndexType indexType)
		{
			// check to see if the index exists
			Node node = client.Nodes[0];
			String result = Info.Request(node, "sindex/" + ns);
			bool indexExists = result.Contains(indexName) &&
									 result.Contains(set) &&
									 result.Contains(binName);

			// create index
			if (!indexExists)
			{
				IndexTask task = client.CreateIndex(null, ns, set, indexName, binName, indexType, IndexCollectionType.LIST);
				task.Wait();
			}
		}

		public void PrintRecord(Key key, Record record)
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
				Console.WriteLine(String.Format("\t   Digest: {0}", ByteUtil.BytesToHexString(key.digest)));
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
