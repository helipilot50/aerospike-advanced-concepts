package aerospike.geo;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;



/**
 * @author Peter Milne
 */
public class Program {
	private AerospikeClient client;
	private String ns = "test"; // Aerospike namespace
	private String airportSet = "airport"; // Aerospike airport set name
	private String regionSet = "region"; // Aerospike region set name
	private String nameIndexSet = "name-index"; // Aerospike name index set name
	private String locationBin = "geo-location"; // Aerospike Bin name for geo location
	private String regionBin = "geo-region"; // Aerospike Bin name for geo region
	private WritePolicy writePolicy = null;
	private JSONParser parser = null;

	public Program()
			throws AerospikeException {
		// Establish a connection to Aerospike cluster
		ClientPolicy cPolicy = new ClientPolicy();
		cPolicy.timeout = 500;
		this.client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);

		writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 600; // expire the records in 10 minutes

		parser = new JSONParser();
	}

	public static void main(String[] args) throws AerospikeException {
		try {

			Program as = new Program();

			as.work();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void work() throws Exception {
		System.out.println("***** Geo in Aerospike *****");

		if (client.isConnected()){
			// create an index on geoBin 
			createIndex(airportSet, "geoLocation", locationBin, IndexType.GEO2DSPHERE);
			createIndex(regionSet, "geoRegion", regionBin, IndexType.GEO2DSPHERE);

			// load geo data
			loadData();

			Statement stmt = new Statement();
			stmt.setNamespace(ns);
			stmt.setSetName(airportSet);
			stmt.setBinNames("ICAO", "IATA", "name", "city", "country");

			// Find all airports within 150km of Sydney Latitude: -33.86785, Longitude: 151.20732

			// TODO create a Radius filter using Sydney's location with a radius of 150km

			System.out.println("Airports:");
			queryStatement(stmt);

			// Find all regions that contain this point
			stmt.setSetName(regionSet);
			stmt.setBinNames("name", "type");
		
			String point = String.format("{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", 
					151.20732d, // Longitude
					-33.86785d // Latitude
					);
			
			// TODO create a filter to discover which regions a point is in 
			
			System.out.println("Regions:");
			queryStatement(stmt);


		}

		client.close();
	}

	private void queryStatement(Statement stmt){
		List<Record> records = new ArrayList<Record>();

		long start = System.currentTimeMillis();
		long stop = 0;
		int count = 0;
		RecordSet recordSet = client.query(null, stmt);
		try {
			while (recordSet != null && recordSet.next()) {
				records.add(recordSet.getRecord());
				count++;
			}
		} finally {
			stop = System.currentTimeMillis();
			recordSet.close();
		}
		for (Record record : records){
			printRecord(record);
		}
		System.out.println(String.format("Found %d records in %d ms", count, (stop-start)));

	}

	private void loadData() throws IOException, ParseException{
		int count = 0;
		//only load airports if they do not exist
		if (!client.exists(null, new Key(ns, airportSet, "SYD:YSSY"))) {

			String airportPath = "../../data/airports.csv";
			Reader in = new FileReader(airportPath);
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);

			for (CSVRecord record : records) {

				Long id = Long.parseLong(record.get(0));
				String name = record.get(1);
				String city = record.get(2);
				String country = record.get(3);
				String IATA = record.get(4);
				String ICAO = record.get(5);
				Double lat = Double.parseDouble(record.get(6));
				Double lon = Double.parseDouble(record.get(7));
				Long elevation = Long.parseLong(record.get(8));
				String region = record.get(11);

				Key key = new Key(ns, airportSet, IATA+":"+ICAO);

				client.put(writePolicy, key,
						new Bin("id", id),
						new Bin("name", name),
						new Bin("city", city),
						new Bin("country", country),
						new Bin("IATA", IATA),
						new Bin("ICAO", ICAO),
						new Bin(locationBin, Value.getAsGeoJSON(createPoint(lon, lat))),
						new Bin("elevation", elevation),
						new Bin("region", region)
						);
				count++;
			}
			System.out.println("Loaded: " + count + " airports");
		}

		//only load countries if they do not exist
		if (!client.exists(null, new Key(ns, regionSet, "ZWE"))) {

			String regionPath = "../../data/countries";
			File countriesDirectory = new File(regionPath);

			String[] countryFileNames = countriesDirectory.list(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".json");
				}
			});

			count = 0;

			for (String countryFileName : countryFileNames){

				JSONObject jsonCountry = (JSONObject) parser.parse(new FileReader(regionPath +"/"+ countryFileName));
				JSONArray features = (JSONArray)jsonCountry.get("features");
				Iterator<?> it = features.iterator();
				if (it.hasNext()) {  
					JSONObject feature = (JSONObject) it.next();  
					JSONObject properties = (JSONObject) feature.get("properties");
					String id = (String) feature.get("id");
					String type = "country";
					String name = (String) properties.get("name");
					String region = feature.get("geometry").toString();

					Key key = new Key(ns, regionSet, id);

					client.put(writePolicy, key,
							new Bin("id", id),
							new Bin("name", name),
							new Bin("type", type),
							new Bin(regionBin, Value.getAsGeoJSON(region))
							);
					count++;
				}  
			}
			System.out.println("Loaded: " + count + " countries");
		}

		//only load cities if they do not exist
		if (!client.exists(null, new Key(ns, regionSet, "TORSHAVN:1"))) {

			String cityPath = "../../data/cities.geo.json";

			count = 0;

			JSONObject jsonCity = (JSONObject) parser.parse(new FileReader(cityPath));
			JSONArray cityArray = (JSONArray)jsonCity.get("features");
			for (Object obj : cityArray){
				JSONObject city = (JSONObject) obj;
				JSONObject properties = (JSONObject) city.get("properties");
				String name = (String) properties.get("NAME");
				String region = city.get("geometry").toString();

				Key indexKey = new Key(ns, nameIndexSet, name);

				Record record = client.operate(writePolicy, indexKey, 
						Operation.add(new Bin("index-counter", 1)), 
						Operation.get("index-counter"));

				String id = name + ":" + record.getInt("index-counter");

				Key recordKey = new Key(ns, regionSet, id);

				client.put(writePolicy, recordKey,
						new Bin("id", id),
						new Bin("name", name),
						new Bin("type", "city"),
						new Bin(regionBin, Value.getAsGeoJSON(region))
						);

				client.operate(writePolicy, indexKey, MapOperation.put(
						new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), 
						"index-bin", 
						Value.get(id), 
						Value.get(recordKey.digest)));
				count++;
			}
			System.out.println("Loaded: " + count + " cities");
		}
	}


	private String createPoint(double lon, double lat) {
		return String.format("{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", lon, lat);
	}

	private void createIndex(String set, String indexName, String binName, IndexType indexType){
		// check to see if the index exists
		Node node = client.getNodes()[0];
		String result = Info.request(node, "sindex/"+ns);
		boolean indexExists = result.contains(indexName) && 
				result.contains(set) &&
				result.contains(binName);

		// create index
		if (!indexExists){
			IndexTask task = client.createIndex(null, ns, set, indexName, binName, indexType);
			task.waitTillComplete();
		}
	}

	private void printRecord(Record record)
	{
		if (record == null)
		{
			System.out.println("\trecord == null");
		}
		else
		{
			for (Map.Entry<String, Object> entry : record.bins.entrySet())
			{
				System.out.println(String.format("\t%s = %s", entry.getKey(), entry.getValue().toString()));
			}
			System.out.println();
		}
	}
}
