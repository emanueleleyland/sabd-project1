package query;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Query interface for all the query classes
 */
public interface Query extends Serializable {

	/**
	 * Query execution with Spark core API
	 *
	 * @return the query results
	 */
	JavaPairRDD<?, ?>  query();

	/**
	 * Query execution with Spark SQL API
	 *
	 * @return the query results
	 */
	Dataset <Row> querySql();

}
