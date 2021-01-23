package de.itcur.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ElasticIndexRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "A simple SMT to manipulate the index name using a document field.";

    public static final String INDEX_ROUTE_NAME = "configure-me";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(INDEX_ROUTE_NAME,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "Field name for the index name apendix.");

    private static final String PURPOSE = "copying field from value to timestamp";

    private String field;

    @Override
    public R apply(R r) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
