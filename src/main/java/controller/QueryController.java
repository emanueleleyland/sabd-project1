package controller;

import deserializer.DeserialiazerSchema;
import deserializer.Deserializer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.event.Level;
import query.Query;
import query.Query1;
import query.Query2;
import query.Query3;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import util.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * the all queries controller
 */
public class QueryController {

    private SparkSession sparkSession;

    /**
     * the QueryController constructor
     *
     * @param sparkSession of the actual spark exection
     */
    public QueryController(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * Computes the Welford online average and variance at the nth step
     *
     * @param stats double array [average, variance] at previous step
     * @param meas  value at the new step
     * @param n     actual nth step
     * @return new double array of [average, variance]
     */
    private double[] addMeasure(double[] stats, double meas, int n) {
        double mean = stats[0] /*average value*/, M2 = stats[1]; /*variance value*/
        double delta = meas - mean;
        mean += delta / n;
        double delta2 = meas - mean;
        M2 += delta * delta2;
        return new double[]{mean, M2};
    }

    /**
     * This is the core of the application.
     * It executes all the queries for each input file schema: csv, avro and parquet.
     * It collects and saves statistics for each queries.
     */
    public void doQueries() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        //hdfs url where are the files to read and to write
        String hdfs = Configuration.hdfsUrl;

        List<Row> list = new ArrayList<>();

        //iterate for all the data schema used: csv, parquet and avro
        for (DeserialiazerSchema schema : DeserialiazerSchema.values()) {
            Boolean sql = false;

            //iterate for Spark Core API and Spark SQL API query
            for (int j = 0; j < 2; j++) {

                //Iterate for all the queries (1, 2, 3)
                for (int i = 1; i <= 3; i++) {

                    //-------------------------------------DEBUG PRINTS-------------------------------------
                    System.err.println("executing query " + i + " schema " + schema + " sql: " + sql);
                    sparkSession.log().warn("executing query " + i + " schema " + schema + " sql: " + sql);
                    //--------------------------------------------------------------------------------------

                    //array to store average and variance values
                    double[] stats = {0.0, 0.0};

                    //this is for use for loop to execute the queries
                    Method doQuery = QueryController.class.getMethod("doQuery" + i, DeserialiazerSchema.class, boolean.class, int.class);

                    //iterate for all the runs of each kind of query to computes statistics
                    for (int k = 1; k <= Configuration.runs; k++) {


                        //get timestamp before the query start
                        Long before = System.currentTimeMillis();

                        //execute the query
                        doQuery.invoke(this, schema, sql, k);

                        //get timestamp after the query end
                        Long after = System.currentTimeMillis();

                        //how much the query takes
                        double measure = after.doubleValue() - before.doubleValue();


                        //compute Welford statistics
                        stats = addMeasure(stats, measure, k - 2);

                        System.err.println("avg: " + (stats[0] / 1000) + " std: " + (Math.sqrt(stats[1] / Configuration.runs) / 1000));
                    }

                    //add Row with query runs info
                    list.add(RowFactory.create(i, schema.toString(), sql, (Configuration.runs - 2), stats[0] / 1000, Math.sqrt(stats[1] / (Configuration.runs - 2) / 1000)));

                }
                sql = true;
            }
        }

        //the schema of the table where the query statistics runs are stored
        StructType schemata = DataTypes.createStructType(
                new StructField[]{
                        createStructField("Query", DataTypes.IntegerType, true),
                        createStructField("format", DataTypes.StringType, true),
                        createStructField("sql", DataTypes.BooleanType, true),
                        createStructField("runs", DataTypes.IntegerType, true),
                        createStructField("avg", DataTypes.DoubleType, true),
                        createStructField("stddev", DataTypes.DoubleType, true),
                });

        //create the table
        Dataset<Row> statistics = sparkSession.sqlContext().createDataFrame(list, schemata);

        //write the table in hdfs
        Deserializer.serializeDataset(statistics, hdfs + "/stats/" + System.currentTimeMillis() + ".csv");
    }

    /**
     * Executes the first query: reads from hdfs the input files, processes them and writes the results to hdfs
     *
     * @param schema    the inpust file schema: csv, avro or parquet
     * @param sql       if the query has to be executed by Spark core or Spark SQL
     * @param iteration how many times it has to be executed
     */
    public void doQuery1(DeserialiazerSchema schema, boolean sql, int iteration) {

        Dataset<Row> dataset = null, countyDataset = null;
        JavaRDD<Row> javaRDD = null, cityJavaRDD = null;
        Dataset<Row> resultsDS = null;
        String filename = null;

        //hdfs url where are the files to read and to write
        String hdfs = Configuration.hdfsUrl;

        //file schema used
        String type = schema.toString();

        //datasets loaded by the files in hdfs
        dataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/weather_description." + type, schema);
        countyDataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/city_attributes." + type, schema);

        //Spark SQL API query
        if (sql) {

            //execute query
            Query1 query1 = new Query1(dataset, countyDataset);
            resultsDS = query1.querySql();

            //filename of the results to write to hdfs
            filename = query1.toString() + "_sql_" + schema.toString() + "_" + iteration;

            //Spark SQL API query
        } else {

            //JavaRDDs loaded by the files in hdfs
            javaRDD = dataset.toJavaRDD();
            cityJavaRDD = countyDataset.toJavaRDD();

            //execute query
            Query1 query1 = new Query1(cityJavaRDD, javaRDD);
            JavaPairRDD<Integer, String> results = (JavaPairRDD<Integer, String>) query1.query();

            //filename of the results to write to hdfs
            filename = query1.toString() + "_" + schema.toString() + "_" + iteration;

            //schema of the results to write to hdfs
            StructType schemata = DataTypes.createStructType(
                    new StructField[]{
                            createStructField("Year", DataTypes.IntegerType, true),
                            createStructField("City", DataTypes.StringType, true),
                    });

            //transform javaRDD results to Dataset
            JavaRDD<Row> rows = results.map(t -> RowFactory.create(t._1(), t._2()));
            resultsDS = sparkSession.sqlContext().createDataFrame(rows, schemata);

        }

        //write results file to hdfs
        Deserializer.serializeDataset(resultsDS, hdfs + "/results/" + filename + ".csv");

    }

    /**
     * Executes the second query: reads from hdfs the input files, processes them and writes the results to hdfs
     *
     * @param schema    the inpust file schema: csv, avro or parquet
     * @param sql       if the query has to be executed by Spark core or Spark SQL
     * @param iteration how many times it has to be executed
     */
    public void doQuery2(DeserialiazerSchema schema, boolean sql, int iteration) {

        Dataset<Row> dataset = null, dataset1 = null, dataset2 = null, countyDataset = null;
        JavaRDD<Row> javaRDD = null, javaRDD1 = null, javaRDD2 = null, cityJavaRDD = null;
        Dataset<Row> resultsDS = null;
        String filename = null;

        //hdfs url where are the files to read and to write
        String hdfs = Configuration.hdfsUrl;

        //file schema used
        String type = schema.toString();

        //datasets loaded by the files in hdfs
        dataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/temperature." + type, schema);
        dataset1 = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/humidity." + type, schema);
        dataset2 = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/pressure." + type, schema);
        countyDataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/city_attributes." + type, schema);

        //Spark SQL API query
        if (sql) {

            List<Dataset<Row>> listSql = (new ArrayList<Dataset<Row>>());
            listSql.add(dataset);
            listSql.add(dataset1);
            listSql.add(dataset2);

            //execute query
            Query2 query2 = new Query2(new String[]{"temperature", "humidity", "pressure"}, listSql, countyDataset);
            resultsDS = (Dataset<Row>) query2.querySql();

            //filename of the results to write to hdfs
            filename = query2.toString() + "_sql_" + schema.toString() + "_" + iteration;

            //Spark Core API query
        } else {

            //JavaRDDs loaded by the files in hdfs
            javaRDD = dataset.toJavaRDD();
            javaRDD1 = dataset1.toJavaRDD();
            javaRDD2 = dataset2.toJavaRDD();
            cityJavaRDD = countyDataset.toJavaRDD();

            List<JavaRDD<Row>> list = (new ArrayList<JavaRDD<Row>>());
            list.add(javaRDD);
            list.add(javaRDD1);
            list.add(javaRDD2);

            //execute query
            Query2 query2 = new Query2(new String[]{"temperature", "humidity", "pressure"}, list, cityJavaRDD);
            JavaPairRDD<Tuple3<Integer, Integer, String>,
                    Tuple2<Tuple5<Double, Double, Double, Double, Double>,
                            Tuple2<Tuple5<Double, Double, Double, Double, Double>,
                                    Tuple5<Double, Double, Double, Double, Double>>>> results =
                    (JavaPairRDD<Tuple3<Integer, Integer, String>,
                            Tuple2<Tuple5<Double, Double, Double, Double, Double>,
                                    Tuple2<Tuple5<Double, Double, Double, Double, Double>,
                                            Tuple5<Double, Double, Double, Double, Double>>>>) query2.query();

            //filename of the results to write to hdfs
            filename = query2.toString() + "_" + schema.toString() + "_" + iteration;

            //schema of the results to write to hdfs
            StructType schemata = DataTypes.createStructType(
                    new StructField[]{
                            createStructField("Country", DataTypes.StringType, true),
                            createStructField("Year", DataTypes.IntegerType, true),
                            createStructField("Month", DataTypes.IntegerType, true),
                            createStructField("max(pressure)", DataTypes.DoubleType, true),
                            createStructField("min(pressure)", DataTypes.DoubleType, true),
                            createStructField("avg(pressure)", DataTypes.DoubleType, true),
                            createStructField("stddev(pressure)", DataTypes.DoubleType, true),
                            createStructField("max(humidity)", DataTypes.DoubleType, true),
                            createStructField("min(humidity)", DataTypes.DoubleType, true),
                            createStructField("avg(humidity)", DataTypes.DoubleType, true),
                            createStructField("stddev(humidity)", DataTypes.DoubleType, true),
                            createStructField("max(temperature)", DataTypes.DoubleType, true),
                            createStructField("min(temperature)", DataTypes.DoubleType, true),
                            createStructField("avg(temperature)", DataTypes.DoubleType, true),
                            createStructField("stddev(temperature)", DataTypes.DoubleType, true),
                    });

            //transform javaRDD results to Dataset
            JavaRDD<Row> rows = results.map(t -> {
                Tuple3<Integer, Integer, String> key = t._1();
                Tuple5<Double, Double, Double, Double, Double> pressure = t._2()._1();
                Tuple5<Double, Double, Double, Double, Double> humidity = t._2()._2()._1();
                Tuple5<Double, Double, Double, Double, Double> temperature = t._2()._2()._2();
                return RowFactory.create(key._3(), key._1(), key._2(), pressure._1(), pressure._2(), pressure._3(), pressure._4(), humidity._1(), humidity._2(), humidity._3(), humidity._4(), temperature._1(), temperature._2(), temperature._3(), temperature._4());
            });
            resultsDS = sparkSession.sqlContext().createDataFrame(rows, schemata);

        }

        //write results file to hdfs
        Deserializer.serializeDataset(resultsDS, hdfs + "/results/" + filename + ".csv");
    }

    /**
     * Executes the third query: reads from hdfs the input files, processes them and writes the results to hdfs
     *
     * @param schema    the inpust file schema: csv, avro or parquet
     * @param sql       if the query has to be executed by Spark core or Spark SQL
     * @param iteration how many times it has to be executed
     */
    public void doQuery3(DeserialiazerSchema schema, boolean sql, int iteration) {

        Dataset<Row> dataset = null, countyDataset = null;
        JavaRDD<Row> javaRDD = null, cityJavaRDD = null;
        Dataset<Row> resultsDS = null;
        String filename = null;

        //hdfs url where are the files to read and to write
        String hdfs = Configuration.hdfsUrl;

        //file schema used
        String type = schema.toString();

        //datasets loaded by the files in hdfs
        dataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/temperature." + type, schema);
        countyDataset = Deserializer.deserializeDataset(sparkSession, hdfs + "/data/" + type + "/city_attributes." + type, schema);

        //Spark SQL API query
        if (sql) {

            //execute query
            Query3 query3 = new Query3(dataset, countyDataset);
            resultsDS = (Dataset<Row>) query3.querySql();

            //filename of the results to write to hdfs
            filename = query3.toString() + "_sql_" + schema.toString() + "_" + iteration;

            //Spark Core API query
        } else {

            //JavaRDDs loaded by the files in hdfs
            javaRDD = dataset.toJavaRDD();
            cityJavaRDD = countyDataset.toJavaRDD();

            //execute query
            Query3 query3 = new Query3(cityJavaRDD, javaRDD);
            JavaPairRDD<Tuple2<String, Double>, Tuple2<String, Long>> results = query3.query();

            //filename of the results to write to hdfs
            filename = query3.toString() + "_" + schema.toString() + "_" + iteration;

            //schema of the results to write to hdfs
            StructType schemata = DataTypes.createStructType(
                    new StructField[]{
                            createStructField("Country", DataTypes.StringType, true),
                            createStructField("City", DataTypes.StringType, true),
                            createStructField("Difference", DataTypes.DoubleType, true),
                            createStructField("2016", DataTypes.LongType, true),
                    });

            //transform javaRDD results to Dataset
            JavaRDD<Row> rows = results.map(t -> RowFactory.create(t._1()._1(), t._2()._1(), t._1()._2(), t._2()._2()));
            resultsDS = sparkSession.sqlContext().createDataFrame(rows, schemata);

        }

        //write results file to hdfs
        Deserializer.serializeDataset(resultsDS, hdfs + "/results/" + filename + ".csv");
    }

}
