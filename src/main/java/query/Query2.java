package query;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import static util.SerializableComparator.serialize;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import javax.validation.constraints.NotNull;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * Query 2 implementation class
 * query: get the avergae, standard deviation, minimum and maximum of temperature,
 * pressure and humidity for each country and year
 */
public class Query2 implements Query {

	private String[] columns = null;

	private JavaRDD<Row> cityJavaRDD = null;
	private List<JavaRDD<Row>> JavaRDDs = null;

	private List<Dataset<Row>> datasets = null;
	private Dataset<Row> countryDataset = null;

	/**
	 * class constructor for Spark Core query execution
	 *
	 * @param columns     column header names for each RDD
	 * @param JavaRDD     RDD for each files
	 * @param cityJavaRDD the city_attributes file RDD
	 */
	public Query2(@NotNull String[] columns, List<JavaRDD<Row>> JavaRDD, JavaRDD<Row> cityJavaRDD) {
		for (int i = 0; i < columns.length; i++) {
			if (columns[i] == null) throw new IllegalArgumentException();
		}
		this.columns = columns;
		this.cityJavaRDD = cityJavaRDD;
		this.JavaRDDs = JavaRDD;
	}

	/**
	 * class constructor for Spark SQL query execution
	 *
	 * @param columns        column header names for each Dataset
	 * @param dataset        Dataset for each files
	 * @param countryDataset the city_attributes file Dataset
	 */
	public Query2(@NotNull String[] columns, List<Dataset<Row>> dataset, Dataset<Row> countryDataset) {
		for (int i = 0; i < columns.length; i++) {
			if (columns[i] == null) throw new IllegalArgumentException();
		}
		this.columns = columns;
		this.datasets = dataset;
		this.countryDataset = countryDataset;
	}

	/**
	 * the query 3 logic with Spark Core API for each RDD
	 *
	 * @param cityJavaRDD the city_attributes file RDD
	 * @param JavaRDD     the file RDD
	 * @return the query 2 response for each RDD
	 */
	public JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple5<Double, Double, Double, Double, Double>> singleQuery(JavaRDD<Row> cityJavaRDD, JavaRDD<Row> JavaRDD) {

		//get the tuple <city, <country, utc zone>> from the RDD
		JavaPairRDD<String, Tuple2<String, String>> nations = cityJavaRDD.mapToPair(row -> new Tuple2<String, Tuple2<String, String>>(row.getString(0), new Tuple2<String, String>(row.getString(3), row.getString(4))));

		JavaPairRDD<Tuple3<String, Integer, Integer>, Double> cachedData = JavaRDD

				//get tuple <city, <timestamp, value>> for each rows
				.flatMapToPair(row -> {

					//list of tuples created from each tuple of the original RDD
					List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList();

					//get the country names from the table header
					String[] fields = row.schema().fieldNames();

					//datetime value
					String date;

					//check if the datetime value is recognized reading the file as a string or a timestamp object
					if (row.get(0).getClass().equals(String.class))
						date = row.getString(0);
					else
						date = row.getTimestamp(0).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString();

					//create new tuple for each couple city-value
					for (int i = 1; i < row.size(); i++) {
						if (row.isNullAt(i)) continue;
						list.add(new Tuple2<String, Tuple2<String, Double>>(fields[i].replaceAll("_", " "), new Tuple2<String, Double>(date, (double) row.getDouble(i))));
					}

					//return list of new tuples
					return list.iterator();
				})

				//join the two RDD to merge temperature values and country - UTC time zone
				.join(nations)

				//get the tuple <<country, year, month>, value>
				.mapToPair(tuple -> {
					Tuple2<Tuple2<String, Double>, Tuple2<String, String>> value = tuple._2();
					String zone = value._2()._2();
					String ts = value._1()._1();

					//get the UTC time shifted for each local zone
					ZonedDateTime timestamp = QueryUtils.UTCShift(ts, zone);
					Integer year = timestamp.getYear();
					Integer month = timestamp.getMonthValue();
					String nation = value._2()._1();
					Double temp = value._1()._2();
					return new Tuple2<Tuple3<String, Integer, Integer>, Double>(new Tuple3<String, Integer, Integer>(nation, year, month), temp);
				}).cache();

		JavaPairRDD<Tuple3<Integer, Integer, String>, Double> maxRDD = cachedData

				//for each country, year and month get the max value
				.reduceByKey((f1, f2) -> Math.max(f1, f2))

				//get the tuple <<country, year, month>, max value>
				.mapToPair(tuple -> new Tuple2<Tuple3<Integer, Integer, String>, Double>(new Tuple3<Integer, Integer, String>(tuple._1()._2(), tuple._1()._3(), tuple._1()._1()), tuple._2()));

		JavaPairRDD<Tuple3<Integer, Integer, String>, Double> minRDD = cachedData

				//for each country, year and month get the min value
				.reduceByKey((f1, f2) -> Math.min(f1, f2))

				//get the tuple <<country, year, month>, min value>
				.mapToPair(tuple -> new Tuple2<Tuple3<Integer, Integer, String>, Double>(new Tuple3<Integer, Integer, String>(tuple._1()._2(), tuple._1()._3(), tuple._1()._1()), tuple._2()));

		JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple3<Double, Double, Double>> avgstdRDD = cachedData

				//compute the online welford mean and standard deviation
				.aggregateByKey(new Tuple3<Long, Double, Double>(0L, 0.0, 0.0),
						(agg, val) -> {
							Long count = agg._1() + 1;
							Double mean = agg._2();
							Double M2 = agg._3();
							Double delta = val - mean;
							mean += delta / count;
							Double delta2 = val - mean;
							M2 += delta * delta2;
							return new Tuple3<Long, Double, Double>(count, mean, M2);
						},
						(agg1, agg2) -> {
							Long count1 = agg1._1(), count2 = agg2._1(), count = count1 + count2;
							Double mean1 = agg1._2(), mean2 = agg2._2();
							Double M2_1 = agg1._3(), M2_2 = agg2._3();
							Double delta = mean2 - mean1;
							Double mean = mean1 + delta * count2 / count;
							Double M2 = M2_1 + M2_2 + Math.pow(delta, 2) * count1 * count2 / (count1 + count2);
							return new Tuple3<Long, Double, Double>(count, mean, M2);
						})

				//get the tuple <<country, year, month>, <mean, standard deviation, unbiased standard deviation>>
				.mapToPair(tuple -> {
					Tuple3<String, Integer, Integer> key = tuple._1();
					Long count = tuple._2()._1();
					Double mean = tuple._2()._2();
					Double M2 = tuple._2()._3();
					return new Tuple2<>(key, new Tuple3<Double, Double, Double>(mean, Math.sqrt(M2 / count), count == 1 ? 0.0 : Math.sqrt(M2 / count - 1)));
				}).mapToPair(tuple -> new Tuple2<Tuple3<Integer, Integer, String>, Tuple3<Double, Double, Double>>(new Tuple3<Integer, Integer, String>(tuple._1()._2(), tuple._1()._3(), tuple._1()._1()), new Tuple3<Double, Double, Double>(tuple._2()._1(), tuple._2()._2(), tuple._2()._3())));

		return maxRDD

				//join average with minimum values
				.join(minRDD)

				//join average, minimum and maximum values
				.join(avgstdRDD)

				//get the tuple <<country, year, month>, <max, min, mean, standard deviation, unbiased standard deviation>>
				.mapToPair(tuple -> new Tuple2<Tuple3<Integer, Integer, String>, Tuple5<Double, Double, Double, Double, Double>>(tuple._1(), new Tuple5<Double, Double, Double, Double, Double>(tuple._2()._1()._1(), tuple._2()._1()._2(), tuple._2()._2()._1(), tuple._2()._2()._2(), tuple._2()._2()._3())));

	}

	/**
	 * the query 2 logic with Spark SQL API for each Dataset
	 *
	 * @param dataset        the file Dataset
	 * @param countryDataset the city_attributes file Dataset
	 * @param column         header names for the Dataset
	 * @return
	 */
	public Dataset<Row> singleQuerySql(Dataset<Row> dataset, Dataset<Row> countryDataset, String column) {

		//get the timestamp column header name
		String timestamp = dataset.columns()[0];

		//create the schema of the new dataset to build
		StructType schema = DataTypes.createStructType(
				new StructField[]{
						createStructField(timestamp, DataTypes.StringType, true),
						createStructField("City", DataTypes.StringType, true),
						createStructField(column, DataTypes.DoubleType, true),
				});

		//get the new dataset with the format | timestamp | city | value |
		Dataset<Row> newDataset = QueryUtils.changeDatasetShape(dataset, schema, false);

		//select the column city, country and UTC time zone
		Dataset<Row> countries = countryDataset.select("City", "Country", "TimeUTC");


		Dataset<Row> set = newDataset

				//join the two RDD to merge temperature values and country - UTC time zone
				.join(countries, "City")

				//set all the timestamps to the zone UTC
				.withColumn("utcshift", to_utc_timestamp(col(timestamp), "UTC"))

				//shift all the timestamp based on the relative UTC time zone
				.withColumn("timestamp", from_utc_timestamp(col("utcshift"), col("TimeUTC")))

				//split the timestamp column and get | year | month | and select the country and value columns
				.select(year(col("timestamp")).as("Year"), month(col("timestamp")).as("Month"), col("Country"), col(column))

				//group all the rows by year, month and country
				.groupBy("Year", "Month", "Country")

				//compute the minimum, maximum, mean ans standard deviation values for each year, month and country
				.agg(max(column), min(column), mean(column), stddev(column))

				//order the rows by year and month
				.orderBy(asc("Year"), asc("Month"));

		return set;

	}

	/**
	 * the query 2 merge of all subquery logic using Spark Core API
	 *
	 * @return the query 2 response
	 */
	public JavaPairRDD<?, ?> query() {

		JavaPairRDD<Tuple3<Integer, Integer, String>, ?> first = null;

		//execute all the subqueries for each RDD
		for (int i = 0; i < this.JavaRDDs.size(); i++) {

			//execute the first subquery
			if (first == null) first = singleQuery(this.cityJavaRDD, this.JavaRDDs.get(i)).cache();

				//execute the other subqueries and merge in a unique response
			else first = singleQuery(this.cityJavaRDD, this.JavaRDDs.get(i))

					//join also with null values cause all the subqueries could not share all the same keys
					.fullOuterJoin(first)

					//
					.mapToPair(t -> {
						return new Tuple2<Tuple3<Integer, Integer, String>, Tuple2<?, ?>>
								(t._1(), new Tuple2<>(t._2()._1().isPresent() ? t._2()._1().get() : null, t._2()._2().isPresent() ? t._2()._2().get() : null));
					}).cache();
		}

		//sort tupleas by year, then by month and, finally, by city
		first.sortByKey(serialize((t1, t2) -> {
			Integer y1 = t1._1(), y2 = t2._1();
			Integer m1 = t1._2(), m2 = t2._2();
			String c1 = t1._3(), c2 = t2._3();
			int year = y1.compareTo(y2);
			int month = m1.compareTo(m2);
			int country = c1.compareTo(c2);
			if (year == 0)
				if (month == 0)
					return country;
				else return month;
			else return year;
		}));

		return first;
	}

	/**
	 * the query 2 merge of all subquery logic using Spark SQL API
	 *
	 * @return the query 2 response
	 */
	public Dataset<Row> querySql() {

		Dataset<Row> first = null, second = null, third = null, app1, app2;

		//list of columns for the join
		List<String> list = new ArrayList<>();
		list.add("Country");
		list.add("Month");
		list.add("Year");

		//execute all the subqueries for each Dataset
		for (int i = 0; i < this.datasets.size(); i++) {

			//execute the first subquery
			if (first == null)
				first = singleQuerySql(this.datasets.get(i), this.countryDataset, this.columns[i]).cache();

				//execute the other subqueries and merge in a unique response
			else {

				first = first

						//rename column to execute join
						.withColumnRenamed("Country", "Country1")
						.withColumnRenamed("Year", "Year1")
						.withColumnRenamed("Month", "Month1");

				first = first

						//join the Dataset of the subqueries already done
						.join(singleQuerySql(this.datasets.get(i), this.countryDataset, this.columns[i]),
								col("Country1").equalTo(col("Country"))
										.and(col("Month1").equalTo(col("Month")))
										.and(col("Year1").equalTo(col("Year"))))

						//drop columns with the same values
						.drop("Country1", "Month1", "Year1").cache();
			}
		}

		return first;

	}

	@Override
	public String toString() {
		return "query2";
	}

}
