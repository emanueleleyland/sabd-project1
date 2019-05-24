package query;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Query 1 implementation class
 * query: get all the cities, for each year, with at least 15 sunny days
 * in March, April and May
 */
public class Query1 implements Query {

	private JavaRDD<Row> cityJavaRDD = null;
	private JavaRDD<Row> javaRDD = null;

	private Dataset<Row> dataset = null;
	private Dataset<Row> countryDataset = null;

	//how many hours of "sky is clear" imply a sunny day
	private int threshold = 16;

	/**
	 * class constructor for Spark Core query execution
	 *
	 * @param cityJavaRDD the city_attributes file RDD
	 * @param javaRDD     the weather_description file RDD
	 */
	public Query1(JavaRDD<Row> cityJavaRDD, JavaRDD<Row> javaRDD) {
		this.cityJavaRDD = cityJavaRDD;
		this.javaRDD = javaRDD;
	}

	/**
	 * class constructor for Spark SQL query execution
	 *
	 * @param dataset        the weather_description file Dataset
	 * @param countryDataset the city_attributes file Dataset
	 */
	public Query1(Dataset<Row> dataset, Dataset<Row> countryDataset) {
		this.dataset = dataset;
		this.countryDataset = countryDataset;
	}

	/**
	 * the query 1 logic with Spark Core API
	 *
	 * @return the query response tuple as <year, city>
	 */
	public JavaPairRDD<Integer, String> query() {

		//get the tuple <city, utc zone> from the RDD
		JavaPairRDD<String, String> nations = cityJavaRDD.mapToPair(row -> new Tuple2<String, String>(row.getString(0), row.getString(4)));

		return javaRDD

				//get the tuple <<city>,<weather description, timestamp>>
				.flatMapToPair(row -> {

					//list of tuples created from each tuple of the original RDD
					List<Tuple2<String, Tuple2<String, String>>> list = new ArrayList<>();

					//get the country names from the table header
					String[] fields = row.schema().fieldNames();

					//datetime value
					String date;

					//check if the datetime value is recognized reading the file as a string or a timestamp object
					if (row.get(0).getClass().equals(String.class))
						date = row.getString(0);
					else
						date = row.getTimestamp(0).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString();

					//create new tuple for each couple city-weather temperature
					for (int i = 1; i < row.size(); i++) {
						if (row.isNullAt(i)) continue;
						list.add(new Tuple2<String, Tuple2<String, String>>(fields[i].replaceAll("_", " "), new Tuple2<String, String>(date, row.getString(i))));
					}

					//return list of new tuples
					return list.iterator();
				})

				//join the two RDD to merge weather description and UTC time zone
				.join(nations)

				//get the tuple <<city, year, month, day>, wether description>
				.mapToPair(tuple -> {
					String city = tuple._1();
					String ts = tuple._2()._1()._1();
					String desc = tuple._2()._1()._2();
					String zone = tuple._2()._2();

					//get the UTC time shifted for each local zone
					ZonedDateTime timestamp = QueryUtils.UTCShift(ts, zone);
					return new Tuple2<Tuple4<String, Integer, Integer, Integer>, String>(new Tuple4<String, Integer, Integer, Integer>(city, timestamp.getYear(), timestamp.getMonthValue(), timestamp.getDayOfMonth()), desc);
				})

				//filter only the tuples with:
				//March <= month <= May
				//weather description == sky is clear
				.filter(tuple -> {
					boolean m = tuple._1()._3() >= 3 && tuple._1()._3() <= 5;
					boolean d = tuple._2().equals("sky is clear");
					return m && d;
				})

				//get the tuple <<city, year, month, day>, 1>
				.mapToPair(tuple -> new Tuple2<Tuple4<String, Integer, Integer, Integer>, Integer>(tuple._1(), 1))

				//count for each city and day how many hours of "sky is clear" there have been
				.reduceByKey((v1, v2) -> v1 + v2)

				//filter only the tuples with a number of hours with "sky is clear" value > threshold
				.filter(tuple -> tuple._2() > threshold)

				//get the tuple <<city, year, month>, 1>
				.mapToPair(tuple -> {
					Tuple4<String, Integer, Integer, Integer> t = tuple._1();
					String city = t._1();
					return new Tuple2<Tuple3<String, Integer, Integer>, Integer>(new Tuple3<String, Integer, Integer>(city, t._2(), t._3()), 1);
				})

				//count for each city and month how many sunny days there have been
				.reduceByKey((v1, v2) -> v1 + v2)

				//filter only the tuple with at least 15 sunny days
				.filter(tuple -> tuple._2() >= 15)

				//get the tuple <<city, year>, 1>>
				.mapToPair(tuple -> {
					Tuple3<String, Integer, Integer> t = tuple._1();
					String city = t._1();
					return new Tuple2<Tuple2<String, Integer>, Integer>(new Tuple2<String, Integer>(city, t._2()), 1);
				})

				//sum how many month with at least 15 sunny days have been for each city
				.reduceByKey((v1, v2) -> v1 + v2)

				//filter only the tuple with 3 months (March, April and May) of at least 15 sunny days for each couple year - city
				.filter(tuple -> tuple._2().equals(3))

				//get tuple <year, city>
				.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._1()._2(), tuple._1()._1()))

				//sort by year
				.sortByKey(true);
	}

	/**
	 * the query 1 logic with Spark SQL API
	 *
	 * @return the query response table as | year | city |
	 */
	public Dataset<Row> querySql() {

		//get the timestamp column header name
		String timestamp = dataset.columns()[0];

		//create the schema of the new dataset to build
		StructType schema = DataTypes.createStructType(
				new StructField[]{
						createStructField(timestamp, DataTypes.StringType, true),
						createStructField("City", DataTypes.StringType, true),
						createStructField("description", DataTypes.StringType, true),
				});

		//get the new dataset with the format | timestamp | city | weather description |
		Dataset<Row> newDataset = QueryUtils.changeDatasetShape(dataset, schema, true);

		//select the column city and UTC time zone
		Dataset<Row> countries = countryDataset.select("City", "TimeUTC");


		Dataset<Row> firstData = newDataset

				//join the two RDD to merge temperature values and UTC time zone
				.join(countries, "City")

				//set all the timestamps to the zone UTC
				.withColumn("utcshift", to_utc_timestamp(col(timestamp), "UTC"))

				//shift all the timestamp based on the relative UTC time zone
				.withColumn("timestamp", from_utc_timestamp(col("utcshift"), col("TimeUTC")))

				//split the timestamp column and get | year | month | day | and select the city column
				.selectExpr("YEAR(timestamp) as Year", "MONTH(timestamp) as month", "DAY(timestamp) as day", "City")

				//filter only the rows with:
				//March <= month <= May
				//weather description == sky is clear
				.where("description = 'sky is clear' AND month >= 3 AND month <= 5")

				//group the rows by year, month, day and city
				.groupBy("Year", "month", "day", "City")

				//count all the "sky is clear" hours for each year, month, day and city
				.count()

				//filter the rows with day with number of "sky is clear" hours more than threshold value
				.filter("count > " + threshold);

		Dataset<Row> secondData = firstData

				//select only year, month, day and city columns
				.select("Year", "month", "day", "City")

				//group rows by year, month and city
				.groupBy("Year", "month", "City")

				//count all the sunny day for each year, month and city
				.count()

				//filter only the month with at least 15 sunny days
				.filter("count >= 15");

		Dataset<Row> ret = secondData

				//group rows by year city
				.groupBy("Year", "City")

				//count all the month with at least 15 sunny days
				.count()

				//filter the city and year with 3 "sunny" months (March, April and May)
				.filter("count = 3")

				//select only the columns year and city
				.select("Year", "City")

				//order the rows by the year value
				.orderBy("Year");

		return ret;

	}

	@Override
	public String toString() {
		return "query1";
	}
}
