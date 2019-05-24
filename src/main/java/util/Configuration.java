package util;

/**
 * collection of parameters
 */
public class Configuration {

	/**
	 * URL address of the Spark Master
	 */
	public static String sparkMaster = "local";
	/**
	 * URL address of the HDFS Master
	 */
	public static String hdfsUrl = "hdfs://master.cini-project.cloud:8020";
	//public static String hdfsUrl = "hdfs://localhost:54310";
	/**
	 * How many times queries are going to be repeated
	 */
	public static Integer runs = 1;

}
