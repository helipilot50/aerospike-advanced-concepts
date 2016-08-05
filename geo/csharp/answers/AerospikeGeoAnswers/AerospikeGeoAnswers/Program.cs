using System;
using System.Collections.Generic;
using System.IO;
using Aerospike.Client;
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq.Dynamic;

namespace AerospikeGeoAnswers
{
	class Program
	{
		private AerospikeClient client;
		private String ns = "test"; // Aerospike namespace
		private String airportSet = "airport"; // Aerospike airport set name
		private String regionSet = "region"; // Aerospike region set name
		private String nameIndexSet = "name-index"; // Aerospike name index set name
		private String locationBin = "geo-location"; // Aerospike Bin name for geo location
		private String regionBin = "geo-region"; // Aerospike Bin name for geo region
		private WritePolicy writePolicy = null;


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
			this.client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);

			writePolicy = new WritePolicy(); // Create a WritePolicy
			writePolicy.sendKey = true; // Save the Key on each write
			writePolicy.expiration = 600; // expire the records in 10 minutes



		}
		public void Work()
		{
			Console.WriteLine("***** Geo in Aerospike *****");

			if (client.Connected)
			{
				// create an index on geoBin 
				CreateIndex(airportSet, "geoLocation", locationBin, IndexType.GEO2DSPHERE);
				CreateIndex(regionSet, "geoRegion", regionBin, IndexType.GEO2DSPHERE);

				// load geo data
				LoadData();

				Statement stmt = new Statement();
				stmt.Namespace = ns;
				stmt.SetName = airportSet;
				stmt.BinNames = new string[] { "ICAO", "IATA", "name", "city", "country" };

				// Find all airports within 150km of Sydney Latitude: -33.86785, Longitude: 151.20732

				stmt.SetFilters(Filter.GeoWithinRadius(locationBin,
						151.20732d, // Longitude
						-33.86785d, // Latitude
						150000 // radius in meters
						));

				Console.WriteLine("Airports:");
				QueryStatement(stmt);

				// Find all regions that contain this point
				stmt.SetName = regionSet;
				stmt.BinNames = new string[] { "name", "type" };

				String point = String.Format("{{ \"type\": \"Point\", \"coordinates\": [{0}, {1}] }}",
						151.20732d, // Longitude
						-33.86785d // Latitude
						);
				stmt.SetFilters(Filter.GeoContains(regionBin, point));

				Console.WriteLine("Regions:");
				QueryStatement(stmt);


			}

			client.Close();
		}

		private void QueryStatement(Statement stmt)
		{
			List<Record> records = new List<Record>();

			long start = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
			long stop = 0;
			int count = 0;
			RecordSet recordSet = client.Query(null, stmt);
			try
			{
				while (recordSet != null & recordSet.Next())

				{
					records.Add(recordSet.Record);
					count++;
				}
			}
			finally
			{
				stop = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;

				if (recordSet != null) recordSet.Close();
			}
			foreach (Record record in records)
				PrintRecord(record);

			Console.WriteLine("Found {0} records in {1} ms", count, (stop - start));

		}

		private void LoadData()
		{
			int count = 0;

			//only load airports if they do not exist
			if (!client.Exists(null, new Key(ns, airportSet, "SYD:YSSY")))
			{
				String airportPath = "../../../../../../data/airports.csv";
				using (TextFieldParser csvParser = new TextFieldParser(airportPath))
				{
					csvParser.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
					csvParser.SetDelimiters(",");
					while (!csvParser.EndOfData)
					{
						//Process row
						string[] fields = csvParser.ReadFields();
						long id = Int64.Parse(fields[0]);
						String name = fields[1];
						String city = fields[2];
						String country = fields[3];
						String IATA = fields[4];
						String ICAO = fields[5];
						Double lat = Double.Parse(fields[6]);
						Double lon = Double.Parse(fields[7]);
						long elevation = Int64.Parse(fields[8]);
						String region = fields[11];

						Key key = new Key(ns, airportSet, IATA + ":" + ICAO);

						client.Put(writePolicy, key,
								new Bin("id", id),
								new Bin("name", name),
								new Bin("city", city),
								new Bin("country", country),
								new Bin("IATA", IATA),
								new Bin("ICAO", ICAO),
								new Bin(locationBin, Value.GetAsGeoJSON(CreatePoint(lon, lat))),
								new Bin("elevation", elevation),
								new Bin("region", region)
								);
						count++;

					}
					Console.WriteLine("Loaded: " + count + " airports");
				}
			}

			//only load countries if they do not exist

			if (!client.Exists(null, new Key(ns, regionSet, "ZWE")))
			{

				String regionPath = "../../../../../../data/countries";
				count = 0;

				foreach (string file in Directory.EnumerateFiles(regionPath, "*.json"))
				{

					string contents = File.ReadAllText(file);
					dynamic jsonCountry = JValue.Parse(contents);

					String id = jsonCountry.features[0].id;
					String type = "country";
					String name = jsonCountry.features[0].properties.name;
					String region = jsonCountry.features[0].geometry.ToString();

					Key key = new Key(ns, regionSet, id);

					client.Put(writePolicy, key,
							new Bin("id", id),
							new Bin("name", name),
							new Bin("type", type),
							new Bin(regionBin, Value.GetAsGeoJSON(region))
							);
						
					count++;
				}
				Console.WriteLine("Loaded: " + count + " countries");
			}

			//only load cities if they do not exist

			if (!client.Exists(null, new Key(ns, regionSet, "TORSHAVN:1")))
			{

				String cityPath = "../../../../../../data/cities.geo.json";
				string contents = File.ReadAllText(cityPath);
				dynamic jsonCity = JValue.Parse(contents);

				foreach (dynamic city in jsonCity.features)
				{
					String name = city.properties.NAME;
					String region = city.geometry.ToString();

					Key indexKey = new Key(ns, nameIndexSet, name);

					Record record = client.Operate(writePolicy, indexKey,
						Operation.Add(new Bin("index-counter", 1)),
						Operation.Get("index-counter"));

					String id = name + ":" + record.GetInt("index-counter");

					Key recordKey = new Key(ns, regionSet, id);

					client.Put(writePolicy, recordKey,
							new Bin("id", id),
							new Bin("name", name),
							new Bin("type", "city"),
							new Bin(regionBin, Value.GetAsGeoJSON(region))
							);

					client.Operate(writePolicy, indexKey, MapOperation.Put(
							new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE),
							"index-bin",
							Value.Get(id),
							Value.Get(recordKey.digest)));
					count++;
				}
				Console.WriteLine("Loaded: " + count + " cities");
			}


		}


		private String CreatePoint(double lon, double lat)
		{
			return String.Format("{{ \"type\": \"Point\", \"coordinates\": [{0}, {1}] }}", lon, lat);
		}

		private void CreateIndex(String set, String indexName, String binName, IndexType indexType)
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
				IndexTask task = client.CreateIndex(null, ns, set, indexName, binName, indexType);
				task.Wait();
			}
		}

		private void PrintRecord(Record record)
		{
			if (record == null)
			{
				Console.WriteLine("\trecord == null");
			}
			else
			{
				foreach (KeyValuePair<string, Object> entry in record.bins)
				{
					Console.WriteLine(String.Format("\t{0} = {1}", entry.Key, entry.Value));
				}
				Console.WriteLine();
			}
		}
	}
}
