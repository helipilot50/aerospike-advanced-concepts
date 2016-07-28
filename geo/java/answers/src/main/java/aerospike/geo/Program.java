package aerospike.geo;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
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
	private String set = "geo"; // Aerospike set name
	private String geoBin = "geo-location"; // Aerospike Bin name for geo location
	private WritePolicy writePolicy = null;

	public Program()
			throws AerospikeException {
		// Establish a connection to Aerospike cluster
		ClientPolicy cPolicy = new ClientPolicy();
		cPolicy.timeout = 500;
		this.client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);

		writePolicy = new WritePolicy(); // Create a WritePolicy
		writePolicy.sendKey = true; // Save the Key on each write
		writePolicy.expiration = 300; // expire the records in 5 minutes
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
		System.out.println("***** Geo in Aerospike *****");

		if (client.isConnected()){
			// create an index on geoBin 
			createIndex("jobLocation", geoBin, IndexType.GEO2DSPHERE);

			// load geo data
			loadData();

			//	Points within Region — Circle Query

			Statement stmt = new Statement();
			stmt.setNamespace(ns);
			stmt.setSetName(set);
			
			//  Find all airports within 50km of Sydney Latitude: -33.86785, Longitude: 151.20732
			stmt.setFilters(Filter.geoWithinRadius(geoBin, 
					151.20732d, // Longitude
					-33.86785d, // Latitude
					50000 // radius in meters
					)); 

			List<Record> airports = new ArrayList<Record>();
			
			long start = System.currentTimeMillis();
			long stop = 0;
			int count = 0;
			RecordSet recordSet = client.query(null, stmt);
			try {
				while (recordSet != null && recordSet.next()) {
					airports.add(recordSet.getRecord());
					count++;
				}
			} finally {
				stop = System.currentTimeMillis();
				recordSet.close();
			}
			for (Record record : airports){
				printRecord(record);
			}
			System.out.println(String.format("Found %d airports in %d ms", count, (stop-start)));
		}

		client.close();
	}

	public void loadData() throws IOException{
		//only load the data if it does not exist
		if (client.exists(null, new Key(ns, set, "SYD:YSSY"))) return;

		String airportPath = "../../data/airports.csv";
		Reader in = new FileReader(airportPath);
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
		int count = 0;
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

			Location location = new Location(lon, lat);

			Key key = new Key(ns, set, IATA+":"+ICAO);

			client.put(writePolicy, key,
					new Bin("id", id),
					new Bin("name", name),
					new Bin("city", city),
					new Bin("country", country),
					new Bin("IATA", IATA),
					new Bin("ICAO", ICAO),
					new Bin(geoBin, Value.getAsGeoJSON(location.toGeoJSONPointDouble())),
					new Bin("elevation", elevation),
					new Bin("region", region)
					);
			count++;
		}
		System.out.println("Loaded: " + count + " airports");
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
			IndexTask task = client.createIndex(null, ns, set, indexName, binName, indexType);
			task.waitTillComplete();
		}
	}

	public void printRecord(Record record)
	{
		if (record == null)
		{
			System.out.println("\trecord == null");
		}
		else
		{
			System.out.println(String.format("Airport: %s %s", record.getString("IATA"), record.getString("ICAO")));	
			for (Map.Entry<String, Object> entry : record.bins.entrySet())
			{
				System.out.println(String.format("\t%s = %s", entry.getKey(), entry.getValue().toString()));
			}
		}
	}

}
