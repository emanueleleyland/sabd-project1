
import controller.QueryController;
import controller.SparkController;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import util.Configuration;

import java.lang.reflect.InvocationTargetException;

/**
 * the main class with the program entrypoint
 */
public class Main {

	/**
	 * program entrypoint
	 *
	 * @param args spark master url, hdfs master url, number of queries repetitions
	 */
	public static void main(String[] args) {

		if (args.length == 1) {
			Configuration.hdfsUrl = args[0];
		} else if (args.length == 2) {
			Configuration.hdfsUrl = args[0];
			Configuration.runs = Integer.valueOf(args[1]);
		}

		System.out.println("STARTED");

		//Create a SparkContext to initialize
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("SABD Project 1");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		// Create a Java version of the Spark Context
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.setLogLevel("WARN");
		SparkSession sparkSession = SparkController.getSparkSession();

		QueryController queryController = new QueryController(sparkSession);

		try {
			queryController.doQueries();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		sc.stop();

		System.out.println("ENDED");
	}


}
