package controller;

import org.apache.spark.sql.SparkSession;

public class SparkController {

    /**
     * get the spark session
     *
     * @return the actual spark session
     */
    public static SparkSession getSparkSession() {

        //return the spark session
        return SparkSession
                .builder()
                .appName("SABD Project 1")
                .getOrCreate();
    }

}
