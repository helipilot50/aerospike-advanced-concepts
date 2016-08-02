package aerospike.scala

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.functions._


object Answers extends App{
	val sc = new SparkContext(new SparkConf().setAppName("AerospikeSpark").setMaster("local[*]"))
	val sqlContext = new SQLContext(sc)


	/*
	 * Read flights data from CSV file
	 */
	val rawFlightsRDD = sc.textFile("../data/Flights 2016-01.csv")
	/*
	 * Parse each line into a Flight case class RDD
	 */
	val flightsRDD = rawFlightsRDD
    	.filter(!_.contains("YEAR")) // Ignore headers
    	.map(_.replace("\"", ""))
    	.map(Flight.assign(_))
    	.filter(_.DEP_TIME != null) // flights that never depart
    	.filter(_.ARR_TIME != null) // flights that never arrive
    	    
	/*
	 * make a DataFrame from the RDD 
	 */
	var flightsDF = sqlContext.createDataFrame(flightsRDD)

	flightsDF.printSchema()
	//flightsDF.show(50)
	
		/*
		 * Save the DataFrame to Aerospike in
		 * Namespace: test
		 * Set: spark-key
		 * key column: key
		 * ttl column: expiry - expire in 300 seconds
		 */

	println("Save flights to Aerospike")
	flightsDF.write.
		mode(SaveMode.Overwrite).
		format("com.aerospike.spark.sql").
		option("aerospike.seedhost", "127.0.0.1").
		option("aerospike.port", "3000").
		option("aerospike.namespace", "test").
		option("aerospike.set", "spark-test").
		option("aerospike.updateByKey", "key").
		option("aerospike.ttlColumn", "expiry").
		save()                
	
	/*
	 * find all the flights that are late
	 */
	println("Find late flights from Aerospike")
	flightsDF = sqlContext.read.
  	format("com.aerospike.spark.sql").
  	option("aerospike.seedhost", "127.0.0.1").
  	option("aerospike.port", "3000").
  	option("aerospike.namespace", "test").
  	option("aerospike.set", "spark-test").
  	load 
	flightsDF.registerTempTable("Flights")
	
	val lateFlightsDF = sqlContext.sql("select CARRIER, FL_NUM, DEP_DELAY_NEW, ARR_DELAY_NEW from Flights")  
	
	println("Flights %d".format(lateFlightsDF.count))
}

/**
 * Class to encapsulate a flight
 */
case class Flight(
		YEAR: Int,
		MONTH: Int,
		DAY_OF_MONTH: Int,
		DAY_OF_WEEK: Int,
		FL_DATE:java.sql.Date,
		CARRIER: String,
		TAIL_NUM: String,
		FL_NUM: String,
		ORIG_ID: Int,
		ORIG_SEQ_ID: Int,
		ORIGIN: String,
		DEST_AP_ID: Int,
		DEST: String,
		DEP_TIME: java.sql.Date,
		DEP_DELAY_NEW: Double,
		ARR_TIME: java.sql.Date,
		ARR_DELAY_NEW: Double,
		ELAPSED_TIME: Double,
		DISTANCE: Double,
		key: String,
		expiry: Int
		) extends Serializable

object Flight{
	val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
			val tf = DateTimeFormat.forPattern("yyyy-MM-dd HHmm")

			def assign(csvRow: String): Flight = {

					val values = csvRow.split(",")
							var flight = new Flight(
									values(0).toInt,
									values(1).toInt,
									values(2).toInt,
									values(3).toInt,
									new java.sql.Date(dtf.parseDateTime(values(4)).getMillis),
									values(5),
									values(6),
									values(7),
									values(8).toInt,
									values(9).toInt,
									values(10),
									values(11).toInt,
									values(12),
									toTime(values(4), values(13)),
									toDouble(values(14)),
									toTime(values(4), values(15)),
									toDouble(values(16)),
									toDouble(values(17)),
									toDouble(values(18)),
									formKey(values),
									300
									)
							flight
	}

	def formKey(values:Array[String]): String = {
			val dep = if (values(13).isEmpty) "XXXX" else values(13)
					values(5)+values(7)+":"+values(4)+":"+dep
	}

	def toDouble(doubleString: String): Double = {
			val number = doubleString match {
			case "" => 0.0
			case _ => doubleString.toDouble
			}
			number
	}

	def toTime(dateString:String, timeString: String): java.sql.Date = {
			val time = timeString match {
			case "" => null
			case "2400" => new java.sql.Date(tf.parseDateTime(dateString + " 0000").getMillis)
			case _ => new java.sql.Date(tf.parseDateTime(dateString + " " + timeString).getMillis)
			}
			time
	}
}

/*
 * Example data
 * 
 * "YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","FL_DATE","CARRIER","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID","ORIGIN","DEST_AIRPORT_ID","DEST","DEP_TIME","DEP_DELAY_NEW","ARR_TIME","ARR_DELAY_NEW","ACTUAL_ELAPSED_TIME","DISTANCE",
 * 2016,1,6,3,2016-01-06,"AA","N4YBAA","43",11298,1129804,"DFW",11433,"DTW","1057",0.00,"1432",0.00,155.00,986.00,
 * 2016,1,7,4,2016-01-07,"AA","N434AA","43",11298,1129804,"DFW",11433,"DTW","1056",0.00,"1426",0.00,150.00,986.00,
 * 2016,1,8,5,2016-01-08,"AA","N541AA","43",11298,1129804,"DFW",11433,"DTW","1055",0.00,"1445",7.00,170.00,986.00,
 * 2016,1,9,6,2016-01-09,"AA","N489AA","43",11298,1129804,"DFW",11433,"DTW","1102",2.00,"1433",0.00,151.00,986.00,
 * 2016,1,10,7,2016-01-10,"AA","N439AA","43",11298,1129804,"DFW",11433,"DTW","1240",100.00,"1631",113.00,171.00,986.00,
 * 
 */
