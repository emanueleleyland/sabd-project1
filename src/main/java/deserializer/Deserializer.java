package deserializer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * Utilities to serialize and deserialize files
 */
public class Deserializer {

	/**
	 * get the DataFrameReader
	 *
	 * @param sparkSession of the actual Spark execution
	 * @return a DataFrameReader
	 */
	private static DataFrameReader getDataFrameReader(@NotNull SparkSession sparkSession) {
		return sparkSession.read();
	}

	/**
	 * Deserialize a JavaRDD from a file
	 *
	 * @param sparkSession of the actual Spark execution
	 * @param path where the file is
	 * @param type schema of the input file
	 * @return the file as a JavaRDD
	 */
	public static JavaRDD<Row> deserializeRDD(@NotNull SparkSession sparkSession, String path, DeserialiazerSchema type) {

		Dataset<Row> dataset = null;

		//get Dataset from the file
		dataset = deserializeDataset(sparkSession, path, type);

		//return the javaRDD representation of the Dataset
		return dataset.javaRDD();
	}

	/**
	 * Deserialize a dataset from a file
	 *
	 * @param sparkSession of the actual Spark execution
	 * @param path where the file is
	 * @param type schema of the input file
	 * @return the file as a Dataset
	 */
	public static Dataset<Row> deserializeDataset(@NotNull SparkSession sparkSession, String path, DeserialiazerSchema type) {

		DataFrameReader reader = getDataFrameReader(sparkSession);

		Dataset<Row> dataset = null;

		switch (type) {

			//read file formatted in Apache Avro
			case avro:
				dataset = reader.format("avro")
						.load(path);
				break;

			//read file formatted in Apache Parquet
			case parquet:
				dataset = reader.parquet(path);
				break;

			//read file formatted in CSV
			case csv:
				dataset = reader.option("inferschema", "true")
						.option("header", "true")
						.csv(path);
				break;

			default:
				return null;

		}

		return dataset;
	}

	/**
	 * Writes results at the given path in csv format
	 *
	 * @param dataset to write
	 * @param path where dataset has to be written
	 */
	public static void serializeDataset(Dataset<Row> dataset, String path) {

		//write CSV file using only one worker
		dataset.coalesce(1).write().format("csv").option("header", "true").save(path);

	}

}
