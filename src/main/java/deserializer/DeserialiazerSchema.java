package deserializer;

/**
 * enum of the schema used for the input files
 */
public enum DeserialiazerSchema {

    csv, /*Comma Separated Value*/
    parquet, /*Apache Parquet format*/
    avro; /*Apache Avro format*/

    @Override
    public String toString() {
        return name();
    }
}
