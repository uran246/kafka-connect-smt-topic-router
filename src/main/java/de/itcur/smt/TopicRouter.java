package de.itcur.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class TopicRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "A simple SMT to manipulate the index name using a document field.";

    public static final String TOPIC_ROUTER_NAME = "topic.appendix.field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_ROUTER_NAME,
                    ConfigDef.Type.STRING,
                    "changeMe",
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "Field name for the topic name appendix.");

    private static final String PURPOSE = "Append fieldvalue to topicname";

    private String field;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        field = config.getString(TOPIC_ROUTER_NAME);
    }

    @Override
    public R apply(R record) {
        String topic;
        Object topicAppendix;

        if (record.valueSchema() == null) {
            final Map<String, Object> value = requireMap(record.value(), PURPOSE);
            topicAppendix = value.get(field);
        } else {
            final Struct value = requireStruct(record.value(), PURPOSE);
            topicAppendix = value.get(field);
        }
        if(topicAppendix instanceof java.lang.String) {
            if (((String) topicAppendix).isEmpty() || topicAppendix == null ) {
                topic = record.topic() + "-undefined";
            } else {
                topic = record.topic() + "-"  + topicAppendix;
            }
        } else {
            topic = record.topic();
        }
        return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
