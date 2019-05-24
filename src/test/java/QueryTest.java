import controller.SparkController;
import deserializer.DeserialiazerSchema;
import deserializer.Deserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import query.Query1;
import query.Query2;
import query.Query3;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * class to test correspondence between query results
 * obtained using Spark Core API and Spark SQL API
 */
class QueryTest {

	static SparkSession sparkSession;
	static JavaSparkContext sc;

	@BeforeAll
	public static void init() {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("Word Count");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		sparkSession = SparkController.getSparkSession();
	}

	@ParameterizedTest
	@EnumSource(
			value = DeserialiazerSchema.class,
			names = {"csv", "avro", "parquet"})
	public void testQuery1(DeserialiazerSchema type) {
		String format = type.toString();

		Dataset<Row> dataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/weather_description." + format, type).cache();
		Dataset<Row> countyDataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/city_attributes." + format, type).cache();


		JavaRDD<Row> javaRDD = dataset.toJavaRDD();
		JavaRDD<Row> cityJavaRDD = countyDataset.toJavaRDD();

		Query1 query1 = new Query1(cityJavaRDD, javaRDD);
		JavaPairRDD<Integer, String> queryResults = query1.query();
		query1 = new Query1(dataset, countyDataset);
		JavaPairRDD<Integer, String> querySqlResults = query1.querySql().toJavaRDD().mapToPair(row -> new Tuple2<>(row.getInt(0), row.getString(1)));
		Assertions.assertEquals(queryResults.count(), querySqlResults.count());

		List<Tuple2<Integer, String>> list = new ArrayList<>();
		List<Tuple2<Integer, String>> listSql = new ArrayList<>();

		queryResults.collect().forEach(t -> list.add(t));
		querySqlResults.collect().forEach(t -> listSql.add(t));

		list.sort((t1, t2) -> {
			if (t1._1().compareTo(t2._1()) == 0) return t1._2().compareTo(t2._2());
			else return t1._1().compareTo(t2._1());
		});

		listSql.sort((t1, t2) -> {
			if (t1._1().compareTo(t2._1()) == 0) return t1._2().compareTo(t2._2());
			else return t1._1().compareTo(t2._1());
		});

		for (int i = 0; i < list.size(); i++) {
			Assertions.assertEquals(list.get(i), listSql.get(i));
		}
	}

	@ParameterizedTest
	@EnumSource(
			value = DeserialiazerSchema.class,
			names = {"csv", "avro", "parquet"})
	public void testQuery2(DeserialiazerSchema type) {

		String format = type.toString();
		Dataset<Row> dataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/temperature." + format, type);
		Dataset<Row> dataset1 = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/humidity." + format, type);
		Dataset<Row> dataset2 = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/pressure." + format, type);
		Dataset<Row> countyDataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/city_attributes." + format, type);
		JavaRDD<Row> javaRDD = dataset.toJavaRDD();
		JavaRDD<Row> javaRDD1 = dataset1.toJavaRDD();
		JavaRDD<Row> javaRDD2 = dataset2.toJavaRDD();
		JavaRDD<Row> cityJavaRDD = countyDataset.toJavaRDD();

		List<JavaRDD<Row>> list = (new ArrayList<JavaRDD<Row>>());
		list.add(javaRDD);
		list.add(javaRDD1);
		list.add(javaRDD2);



		Query2 query2 = new Query2(new String[]{"temperature", "humidity", "pressure"}, list, cityJavaRDD);
		JavaPairRDD<Tuple3<Integer, Integer, String>,
				Tuple2<Tuple5<Double, Double, Double, Double, Double>,
						Tuple2<Tuple5<Double, Double, Double, Double, Double>,
								Tuple5<Double, Double, Double, Double, Double>>>> query2Results =
				(JavaPairRDD<Tuple3<Integer, Integer, String>,
						Tuple2<Tuple5<Double, Double, Double, Double, Double>,
								Tuple2<Tuple5<Double, Double, Double, Double, Double>,
										Tuple5<Double, Double, Double, Double, Double>>>>) query2.query();

		JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple3<Tuple5<Double, Double, Double, Double, Double>, Tuple5<Double, Double, Double, Double, Double>, Tuple5<Double, Double, Double, Double, Double>>> results =
				query2Results.mapToPair(t -> {
					Tuple3<Integer, Integer, String> key = t._1();
					Tuple5<Double, Double, Double, Double, Double> pressure = t._2()._1();
					Tuple5<Double, Double, Double, Double, Double> humidity = t._2()._2()._1();
					Tuple5<Double, Double, Double, Double, Double> temperature = t._2()._2()._2();
					return new Tuple2<Tuple3<Integer, Integer, String>, Tuple3<Tuple5<Double, Double, Double, Double, Double>, Tuple5<Double, Double, Double, Double, Double>, Tuple5<Double, Double, Double, Double, Double>>>
							(key,new Tuple3<>(pressure, humidity, temperature));
				});

		List<Dataset<Row>> listSql = (new ArrayList<Dataset<Row>>());
		listSql.add(dataset);
		listSql.add(dataset1);
		listSql.add(dataset2);

		query2 = new Query2(new String[]{"temperature", "humidity", "pressure"}, listSql, countyDataset);
		Dataset<Row> query2SqlResults = query2.querySql();

		JavaPairRDD<Tuple3<Integer, Integer, String>, Tuple3<Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>>> sqlPair =
				query2SqlResults.toJavaRDD()
				.mapToPair(row -> {
					return new Tuple2<Tuple3<Integer, Integer, String>, Tuple3<Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>>>
							(new Tuple3<Integer, Integer, String>(row.getInt(2), row.getInt(1), row.getString(0)),
									new Tuple3<>(new Tuple4<>(row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6)),
									new Tuple4<>(row.getDouble(7), row.getDouble(8), row.getDouble(9), row.getDouble(10)),
									new Tuple4<>(row.getDouble(11), row.getDouble(12), row.getDouble(13), row.getDouble(14))));
				});

		Assertions.assertEquals(query2Results.count(), sqlPair.count());

		Map<Tuple3<Integer, Integer, String>, Tuple3<Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>, Tuple4<Double, Double, Double, Double>>>
				map = sqlPair.collectAsMap();

		results.collect().forEach(t -> {
			Assertions.assertEquals(t._2()._1()._1(), map.get(t._1())._1()._1());
			Assertions.assertEquals(t._2()._1()._2(), map.get(t._1())._1()._2());
			Assertions.assertTrue(Math.abs(t._2()._1()._3() - map.get(t._1())._1()._3()) <= 0.005);
			Assertions.assertTrue(Math.abs(t._2()._1()._4() - map.get(t._1())._1()._4()) <= 0.005);
			Assertions.assertEquals(t._2()._2()._1(), map.get(t._1())._2()._1());
			Assertions.assertEquals(t._2()._2()._2(), map.get(t._1())._2()._2());
			Assertions.assertTrue(Math.abs(t._2()._2()._3() - map.get(t._1())._2()._3()) <= 0.005);
			Assertions.assertTrue(Math.abs(t._2()._2()._4() - map.get(t._1())._2()._4()) <= 0.005);
			Assertions.assertEquals(t._2()._3()._1(), map.get(t._1())._3()._1());
			Assertions.assertEquals(t._2()._3()._2(), map.get(t._1())._3()._2());
			Assertions.assertTrue(Math.abs(t._2()._3()._3() - map.get(t._1())._3()._3()) <= 0.005);
			Assertions.assertTrue(Math.abs(t._2()._3()._4() - map.get(t._1())._3()._4()) <= 0.005);
		});

	}

	@ParameterizedTest
	@EnumSource(
			value = DeserialiazerSchema.class,
			names = {"csv", "avro", "parquet"})
	public void testQuery3(DeserialiazerSchema type) {
		String format = type.toString();

		Dataset<Row> dataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/temperature." + format, type);
		Dataset<Row> countyDataset = Deserializer.deserializeDataset(sparkSession, "hdfs://localhost:54310/data/" + format + "/city_attributes." + format, type);
		JavaRDD<Row> javaRDD = Deserializer.deserializeRDD(sparkSession, "hdfs://localhost:54310/data/" + format + "/temperature." + format, type);
		JavaRDD<Row> cityJavaRDD = Deserializer.deserializeRDD(sparkSession, "hdfs://localhost:54310/data/" + format + "/city_attributes." + format, type);

		Query3 query3 = new Query3(cityJavaRDD, javaRDD);
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Long>> results = query3.query()
				.mapToPair(t -> new Tuple2<Tuple2<String, String>, Tuple2<Double, Long>>(new Tuple2<>(t._1()._1(), t._2()._1()), new Tuple2<>(t._1()._2(), t._2()._2())));
		query3 = new Query3(dataset, countyDataset);
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Long>> sqlResults = query3.querySql()
				.toJavaRDD()
				.mapToPair(row -> new Tuple2<Tuple2<String, String>, Tuple2<Double, Long>>(new Tuple2<>(row.getString(0), row.getString(1)), new Tuple2<>(row.getDouble(2), (long) row.getInt(3))));

		Assertions.assertEquals(results.count(), sqlResults.count());

		Map<Tuple2<String, String>, Tuple2<Double, Long>> map = sqlResults.collectAsMap();

		results.collect().forEach(t -> {
			Assertions.assertTrue(Math.abs(t._2()._1() - map.get(t._1())._1()) <= 0.005);
			Assertions.assertEquals(t._2()._2(), map.get(t._1())._2());
		});
	}

}
