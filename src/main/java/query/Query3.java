package query;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_utc_timestamp;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static util.SerializableComparator.serialize;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Query 3 implementation class
 * query:get the three cities with the greatest difference in average temperature
 * in June, July, August and September with respect to January, February, March and April
 * in the local time slot from 12 AM to 3 PM and compare the results of the years 2016 and 2017
 */
public class Query3 implements Query {

	private JavaRDD<Row> cityJavaRDD = null;
	private JavaRDD<Row> javaRDD = null;

	private Dataset<Row> dataset = null;
	private Dataset<Row> countryDataset = null;

	/**
	 * class constructor for Spark Core query execution
	 *
	 * @param cityJavaRDD the city_attributes file RDD
	 * @param javaRDD     the temperature file RDD
	 */
	public Query3(JavaRDD<Row> cityJavaRDD, JavaRDD<Row> javaRDD) {
		this.cityJavaRDD = cityJavaRDD;
		this.javaRDD = javaRDD;
	}

	/**
	 * class constructor for Spark SQL query execution
	 *
	 * @param dataset        the temperature file Dataset
	 * @param countryDataset the city_attributes file Dataset
	 */
	public Query3(Dataset<Row> dataset, Dataset<Row> countryDataset) {
		this.dataset = dataset;
		this.countryDataset = countryDataset;
	}

	/**
	 * the query 3 logic with Spark Core API
	 *
	 * @return the query response tuple as <<country, 2017's difference value>, <city, 2016's rank position>>
	 */
	public JavaPairRDD<Tuple2<String, Double>, Tuple2<String, Long>> query() {

		//get the tuple <city, <country, utc zone>> from the RDD
		JavaPairRDD<String, Tuple2<String, String>> nations = cityJavaRDD.mapToPair(row -> new Tuple2<String, Tuple2<String, String>>(row.getString(0), new Tuple2<String, String>(row.getString(3), row.getString(4))));


		JavaPairRDD<Tuple2<String, Integer>, Iterable<Tuple2<String, Double>>> cache = javaRDD

				//get tuple <city, <timestamp, temperature value>> for each rows
				.flatMapToPair(row -> {

					//list of tuples created from each tuple of the original RDD
					List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<>();

					//get the country names from the table header
					String[] fields = row.schema().fieldNames();

					//datetime value
					String date;

					//check if the datetime value is recognized reading the file as a string or a timestamp object
					if (row.get(0).getClass().equals(String.class))
						date = row.getString(0);
					else
						date = row.getTimestamp(0).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString();

					//create new tuple for each couple city-temperature
					for (int i = 1; i < row.size(); i++) {
						if (row.isNullAt(i)) continue;
						list.add(new Tuple2<String, Tuple2<String, Double>>(fields[i].replaceAll("_", " "), new Tuple2<String, Double>(date, row.getDouble(i))));
					}

					//return list of new tuples
					return list.iterator();
				})

				//join the two RDD to merge temperature values and country - UTC time zone
				.join(nations)

				//get the tuple <<country, city>, <year, month, hour, temperature value>>
				.mapToPair(tuple -> {
					String ts = tuple._2()._1()._1();
					String city = tuple._1();
					String country = tuple._2()._2()._1();
					Double temp = tuple._2()._1()._2();
					String zone = tuple._2()._2()._2();

					//get the UTC time shifted for each local zone
					ZonedDateTime date = QueryUtils.UTCShift(ts, zone);
					return new Tuple2<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Double>>(new Tuple2<>(country, city), new Tuple4<>(date.getYear(), date.getMonthValue(), date.getHour(), temp));
				})

				//filter only the tuples with:
				//January <= month <= April || June <= month <= September
				//2016 <= year <= 2017
				//12 <= hour <= 15
				.filter(tuple -> {
					Integer year = tuple._2()._1();
					Integer month = tuple._2()._2();
					Integer hour = tuple._2()._3();
					boolean m = (month >= 1 && month <= 4) || (month >= 6 && month <= 9);
					boolean y = year == 2017 || year == 2016;
					boolean h = hour >= 12 && hour <= 15;
					return m && h && y;
				})

				//get tuple <<country, city, month slot, year>, temperature value>
				.mapToPair(tuple -> {
					String country = tuple._1()._1();
					String city = tuple._1()._2();
					String slot = "";
					Double temp = tuple._2()._4();
					Integer month = tuple._2()._2();
					Integer year = tuple._2()._1();
					if (month >= 1 && month <= 4) slot = "jan-apr";
					else slot = "jun-sep";
					return new Tuple2<Tuple4<String, String, String, Integer>, Double>(new Tuple4<String, String, String, Integer>(country, city, slot, year), temp);
				})

				//compute the online welford mean
				.aggregateByKey(new Tuple2<Long, Double>(0L, 0.0), (agg, val) -> {
					Long count = agg._1() + 1;
					Double mean = agg._2();
					Double delta = val - mean;
					mean += delta / count;
					return new Tuple2<Long, Double>(count, mean);
				}, (agg1, agg2) -> {
					Long count1 = agg1._1(), count2 = agg2._1(), count = count1 + count2;
					Double mean1 = agg1._2(), mean2 = agg2._2();
					Double delta = mean2 - mean1;
					Double mean = mean1 + delta * count2 / count;
					return new Tuple2<Long, Double>(count, mean);
				})

				//get the tuple <<country, city, year>, average temperature value>
				.mapToPair(tuple -> new Tuple2<Tuple3<String, String, Integer>, Double>(new Tuple3<String, String, Integer>(tuple._1()._1(), tuple._1()._2(), tuple._1()._4()), tuple._2()._2()))

				//compute the absolute value of the difference between the average temperature in jan-apr and jun-sep
				.reduceByKey((v1, v2) -> (Math.abs(v1 - v2)))

				//get the tuple <<country, year>, <city, absolute average difference of temperature value>>
				.mapToPair(tuple -> new Tuple2<Tuple2<String, Integer>, Tuple2<String, Double>>(new Tuple2<String, Integer>(tuple._1()._1(), tuple._1()._3()), new Tuple2<String, Double>(tuple._1()._2(), tuple._2())))

				//group all the tuples with the same year and country values
				.groupByKey()
				.cache();

		JavaPairRDD<Tuple2<String, String>, Double> c16 = cache

				//filter only the 2017's tuples
				.filter(tuple -> tuple._1()._2() == 2017)

				//get tuple <<country, city>, temperature value>
				.flatMapToPair(tuple -> {

					//list of the first three cities in the rank
					List<Tuple2<Tuple2<String, String>, Double>> resultList = new ArrayList<>();

					//the whole list of cities in 2017
					Iterator<Tuple2<String, Double>> list = tuple._2().iterator();

					//order the list
					List<Tuple2<String, Double>> sortedList = QueryUtils.sortList(list);

					//take only the first three positions
					for (int i = 0; i < 3; i++)
						resultList.add(new Tuple2<Tuple2<String, String>, Double>(new Tuple2<>(tuple._1()._1(), sortedList.get(i)._1()), sortedList.get(i)._2()));

					//return the podium
					return resultList.iterator();
				})
				.cache();

		JavaPairRDD<Tuple2<String, String>, Long> c17 = cache

				//filter only the 2016 tuples
				.filter(tuple -> tuple._1()._2() == 2016)

				//get tuple <<country, city>, position in rank>
				.flatMapToPair(tuple -> {

					//list of the cities in the rank
					List<Tuple2<Tuple2<String, String>, Long>> resultList = new ArrayList<>();

					//the whole list of cities in 2016
					Iterator<Tuple2<String, Double>> list = tuple._2().iterator();

					//order the list
					List<Tuple2<String, Double>> sortedList = QueryUtils.sortList(list);

					//take the whole rank
					for (int i = 0; i < sortedList.size(); i++)
						resultList.add(new Tuple2<Tuple2<String, String>, Long>(new Tuple2<>(tuple._1()._1(), sortedList.get(i)._1()), (long) i + 1));

					//return the rank
					return resultList.iterator();
				})
				.cache();


		return c16

				//join the 2017's podium with the list of ordered
				.join(c17)

				//get the tuple <<country, temperature value in 2017>, <city, rank position in 2016>>
				.mapToPair(tuple -> new Tuple2<Tuple2<String, Double>, Tuple2<String, Long>>(new Tuple2<String, Double>(tuple._1()._1(), tuple._2()._1()), new Tuple2<String, Long>(tuple._1()._2(), tuple._2()._2())))

				//sort tuples by country and then by temperature value to obtain a podium for each country
				.sortByKey(serialize((t1, t2) -> {
					String c1 = t1._1(), c2 = t2._1();
					Double d1 = t1._2(), d2 = t2._2();
					int country = c1.compareTo(c2);
					if (country == 0) return d1.compareTo(d2);
					else return country;
				}), false);

	}


	/**
	 * the query 3 logic with Spark SQL API
	 *
	 * @return the query response table as | country | city | 2017's difference value | 2016's rank position | 2017's rank position |
	 */
	public Dataset<Row> querySql() {

		//get the timestamp column header name
		String timestamp = dataset.columns()[0];

		//create the schema of the new dataset to build
		StructType schema = DataTypes.createStructType(
				new StructField[]{
						createStructField(timestamp, DataTypes.StringType, true),
						createStructField("City", DataTypes.StringType, true),
						createStructField("temperature", DataTypes.DoubleType, true),
				});

		//get the new dataset with the format | timestamp | city | temperature |
		Dataset<Row> newDataset = QueryUtils.changeDatasetShape(dataset, schema, false);

		//select the column city, country and UTC time zone
		Dataset<Row> countries = countryDataset.select("City", "Country", "TimeUTC");

		Dataset<Row> yearSet = newDataset

				//join the two RDD to merge temperature values and country - UTC time zone
				.join(countries, "City")

				//set all the timestamps to the zone UTC
				.withColumn("utcshift", to_utc_timestamp(col(timestamp), "UTC"))

				//shift all the timestamp based on the relative UTC time zone
				.withColumn("timestamp", from_utc_timestamp(col("utcshift"), col("TimeUTC")))

				//split the timestamp column and get | year | month | hour | and select the temperature value
				.select(col("Country"), col("City"), year(col("timestamp")).as("year"), month(col("timestamp")).as("month"), hour(col("timestamp")).as("hour"), col("temperature"))

				//filter for the rows with 2016 <= year <= 2017
				.where(col("year").equalTo("2016").or(col("year").equalTo("2017")))

				//filter for the rows with 12 <= hour <= 15
				.where(col("hour").between(12, 15))
				.cache();


		Dataset<Row> monthSet1 = yearSet

				//filter for January <= month <= April
				.where(col("month").between(1, 4))

				//group rows per country, city and year
				.groupBy("Country", "City", "year")

				//compute the average temperature value
				.agg(avg("temperature"));

		Dataset<Row> monthSet2 = yearSet

				//filter for June <= month <= September
				.where(col("month").between(6, 9))

				//group rows per country, city and year
				.groupBy("Country", "City", "year")

				//compute the average temperature value
				.agg(avg("temperature"));

		//list of columns for the join
		List<String> list = new ArrayList<>();
		list.add("Country");
		list.add("City");
		list.add("year");

		Dataset<Row> moduleSet = monthSet1

				//join of the two dataset with the two average temperature in two different month slots
				.join(monthSet2, JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq())

				//create new column with the resultw of the absolute dififference of the average temperature in the two month slots
				.withColumn("difference", abs(monthSet1.col("avg(temperature)").minus(monthSet2.col("avg(temperature)"))))

				//select only country, city, year and difference columns
				.select("Country", "City", "year", "difference")
				.cache();

		//create a window to order each group of rows with the same country by the difference of average temperature values
		WindowSpec w = Window.partitionBy("Country").orderBy(desc("difference"));

		Dataset<Row> module2017 = moduleSet

				//select only 2017's rows
				.where(col("year").equalTo("2017"))

				//add new column for the rank position in 2016 for each city in each country
				.withColumn("place", row_number().over(w))

				//select only the first three positions
				.where(col("place").between(1, 3))

				//select only country, city nad difference columns
				.select("Country", "City", "difference");

		Dataset<Row> module2016 = moduleSet

				//select only 2016's rows
				.where(col("year").equalTo("2016"))

				//add new column for the rank position in 2016 for each city in each country
				.withColumn("2016", row_number().over(w))

				//select country, city and 2016's rank position
				.select("Country", "City", "2016");

		return module2017

				//join the 2017's podium and the complete 2016's rank
				.join(module2016, module2017.col("Country").equalTo(module2016.col("Country")).and(module2017.col("City").equalTo(module2016.col("City"))))

				//select only country, city, difference and 2016 rank position columns
				.select(module2017.col("Country"), module2017.col("City"), col("difference"), col("2016"))

				//add new column for the rank position in 2017 for each city in each country
				.withColumn("2017", row_number().over(w));
	}

	@Override
	public String toString() {
		return "query3";
	}

}
