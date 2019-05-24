package query;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * collection of functions used by all the queries
 */
public class QueryUtils {

	/**
	 * Get a new Dataset where rows are: | timestamp | city | value |
	 * from Dataset like: | timestamp | valueOfCity1 | valueOfCity2 | valueOfCity3 | ... | valueOfCityN |
	 *
	 *\begin{itemize}
    \item tuple \textit{ <stato, anno, mese, massima temperatura, minima temperatura, temperatura media, deviazione standard della temperatura, deviazione standard non distorta della temperatura, massima umidità, minima umidità, ...>}
\end{itemize} @param dataset the original one
	 * @param schema of the new rows
	 * @param query1 if it's used for query1 or the others
	 * @return the new Dataset
	 */
	public static Dataset<Row> changeDatasetShape(Dataset<Row> dataset, StructType schema, boolean query1) {

		Dataset<Row> newDataset = dataset.flatMap((FlatMapFunction<Row, Row>) (row -> {

			//list of rows created from each row of the original dataset
			List<Row> list = new ArrayList();

			//get the country names from the table header
			String[] fields = row.schema().fieldNames();

			//datetime value
			String date;

			//check if the datetime value is recognized reading the file as a string or a timestamp object
			if (row.get(0).getClass().equals(String.class))
				date = row.getString(0);
			else
				date = row.getTimestamp(0).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString();

			//new row created
			Row r = null;
			for (int i = 1; i < row.size(); i++) {

				//skip null values
				if (row.isNullAt(i)) continue;

				//in query 1 output rows are | timestamp | country | wheather_description |
				if(query1) {
					r = RowFactory.create(date, fields[i].replaceAll("_", " "), row.getString(i));

				//in query 1 output rows are | timestamp | country | temperature/pressure/humidity |
				} else {
					r = RowFactory.create(date, fields[i].replaceAll("_", " "), row.getDouble(i));
				}

				//add new row
				list.add(r);
			}

			//return list of new rows
			return list.iterator();

			//schema of the new rows
		}), RowEncoder.apply(schema));

		//return new dataset
		return newDataset;
	}

	/**
	 * Shift the timestamp given an "UTC+/-0" one and his UTC Zone ID to an "UTC+/-n"
	 *
	 * @param timestamp is an UTC+/-0 time
	 * @param zone is a UTC Zone ID
	 * @return the timestamp shifted
	 */
	public static ZonedDateTime UTCShift(String timestamp, String zone) {

		//get the timestamp value in that format
		LocalDateTime ldt = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

		//set the time zone of the timestamp as UTC
		ZonedDateTime zdt = ldt.atZone(ZoneId.of("UTC"));

		//shift of the time based on given time zone
		ZonedDateTime date = zdt.withZoneSameInstant(ZoneId.of(zone));

		//return shifted timestamp
		return date;
	}

	/**
	 * Order the list of tuples <city, value>
	 *
	 * @param list of tuples <city, value>
	 * @return the list ordered by the value
	 */
	public static List<Tuple2<String, Double>> sortList(Iterator<Tuple2<String, Double>> list) {

		//new list with tuples to sort
		List<Tuple2<String, Double>> sortedList = new ArrayList<>();

		//add tuples to sort to the list
		list.forEachRemaining(t -> sortedList.add(t));

		//sort the list of tuples by the values
		sortedList.sort((t1, t2) -> -t1._2().compareTo(t2._2()));

		//return the sorted list
		return sortedList;
	}



}
