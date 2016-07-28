using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace AerospikeMapsAnswers
{
	class Program
	{
		private string ns = "test"; // Aerospike namespace
		private string set = "maps"; // Aerospike set name
		private string mapBin = "map-of-things"; // Aerospike Bin name for a list
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
			client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);

		}
		public void Work()
		{
			Console.WriteLine("***** Maps in Aerospike *****");

			WritePolicy writePolicy = new WritePolicy(); // Create a WritePolicy
			writePolicy.sendKey = true; // Save the Key on each write
			writePolicy.expiration = 300; // expire the records in 5 minutes

			if (client.Connected)
			{
				// Make a simple map and save it and read the record
				Dictionary<String, Object> aMapOfObjects = new Dictionary<String, Object>(){
					{"ersionsnummer", 1},
					{"ieferant", "B"},
					{"eisebeginn", "016-10-11"},
					{"eiseende", "016-10-11"},
					{"bflughafen_Hin", "AH"},
					{"nkunfsthafen_Hin", "CN"},
					{"luglinie_Hin", "B"},
					{"bflugzeit_Hin", 925},
					{"nkunftszeit_Hin", 1720},
					{"bflugzeit_Rueck", 0},
					{"nkunftszeit_Rueck", 0},
					{"lugnummer_Hin", 385},
					{"aehrung", "UR"},
					{"reis", 861.0f},
					{"nfant_Preis", 126},
					{"lter_von_1", 2},
					{"lter_bis_1", 11},
					{"reis_Kinderstufe1", 61},
					{"lter_von_2", 0},
					{"lter_bis_2", 0},
					{"otelkategorie", 0},
					{"eisetypkürzel", "F"},
					{"eisetyp_Langtext", "ur Flug"}
				};
		

			Key key = new Key(ns, set, "a-record-with-a-map");

			Bin map = new Bin(mapBin, aMapOfObjects);
			client.Put(writePolicy, key, map);

			Record record = client.Get(null, key, mapBin);

			PrintRecord(key, record);

			// Add 1 element to the map and print the result

			MapPolicy mapPolicy = new MapPolicy(
					MapOrder.KEY_ORDERED,
					MapWriteMode.UPDATE_ONLY
					);

			client.Operate(writePolicy, key, MapOperation.Put(mapPolicy, mapBin, Value.Get("cat"), Value.Get(7)));
					
			record = client.Get(null, key, mapBin);
			PrintRecord(key, record);

			// Add elements to the map and read the whole map.

			Dictionary<Value, Value> anotherMap = new Dictionary<Value, Value>(){
					{Value.Get("dogs"), Value.Get(1)},
					{Value.Get("mice"), Value.Get("B")}
			};

			client.Operate(writePolicy, key, 
							MapOperation.PutItems(mapPolicy, mapBin, anotherMap));
					
					record = client.Get(null, key, mapBin);
					PrintRecord(key, record);

			// Delete a key/value from the map and also return new size of map.
			record = client.Operate(writePolicy, key, 
							MapOperation.RemoveByKey(mapBin, Value.Get("dogs"), MapReturnType.KEY),
							MapOperation.Size(mapBin));

			PrintRecord(key, record);

			// Queries on secondary indexes

			// Create many records with values in a map
			Random rand = new Random(300);
				for (int i = 0; i< 100; i++){
					Key newKey = new Key(ns, set, "a-record-with-a-map-" + i);
					Dictionary<Value, Value> aMap = new Dictionary<Value, Value>();
					for ( int j = 0; j< 100; j++){
						aMap[Value.Get(String.Concat("dogs",j))] = Value.Get(rand.Next(250, 350));
						aMap[Value.Get(String.Concat("mice",j))] = Value.Get(rand.Next(250, 350));
					}
					client.Put(writePolicy, newKey, new Bin(mapBin, aMap));
				}

			// Create indexes on map bin, if they do not exist
				CreateIndex("mapKeyIndex", mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
				CreateIndex("mapValueIndex", mapBin, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

				// Query the records with map values between 300 and 350

				// Execute the Query
				Statement stmt = new Statement();
				stmt.Namespace = ns;
				stmt.SetName = set;
				stmt.SetFilters(Filter.Range(mapBin, IndexCollectionType.MAPVALUES, 300, 350));
				
				RecordSet recordSet = client.Query(null, stmt);
				try {
					Console.WriteLine("\nRecords with map values between 300 and 350:");
					while (recordSet != null & recordSet.Next()){
						Console.WriteLine("\t" + recordSet.Key.userKey);
					}
				} finally {
					if (recordSet != null) recordSet.Close();
				}
				
				// Query the records with map values between 300 and 350
				
				// Execute the Query
				stmt = new Statement();
				stmt.Namespace = ns;
				stmt.SetName = set;
				stmt.SetFilters(Filter.Contains(mapBin, IndexCollectionType.MAPKEYS, "dogs7"));
				
				recordSet = client.Query(null, stmt);
				try {
					Console.WriteLine("\nRecords with map keys equal to dogs7:");
					while (recordSet != null & recordSet.Next())
					{
						Console.WriteLine("\t" + recordSet.Key.userKey);
					}
				} finally {
					if (recordSet != null) recordSet.Close();
				}
				


			// close Aerospike at the end of your program
				client.Close();
			}
		}

		public void CreateIndex(String indexName, String binName, IndexType indexType, IndexCollectionType collectionType)
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
				IndexTask task = client.CreateIndex(null, ns, set, indexName, binName, indexType, collectionType);
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
