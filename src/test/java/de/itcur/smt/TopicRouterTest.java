
/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.itcur.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TopicRouterTest {

    private final TopicRouter<SourceRecord> xform = new TopicRouter();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("topic.appendix.field", "TopicRoute"));
        xform.apply(new SourceRecord(null, null, "test", 0, Schema.OPTIONAL_STRING_SCHEMA, "appendix"));
    }

    @Test
    public void copySchemaAndTopicRouterField() {
        final Map<String, Object> props = new HashMap<>();

        props.put("topic.appendix.field", "TopicRoute");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("TopicRoute", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("TopicRoute", "appendix");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("TopicRoute").schema());
        assertEquals("appendix", ((Struct) transformedRecord.value()).getString("TopicRoute").toString());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("TopicRoute").schema());
        assertEquals("test-appendix", transformedRecord.topic());
        assertNotNull(((Struct) transformedRecord.value()).getString("TopicRoute"));

        // Exercise caching
        final SourceRecord transformedRecord2 = xform.apply(
                new SourceRecord(null, null, "test-appendix", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }

    @Test
    public void copySchemaAndTopicRouterFieldEmpty() {
        final Map<String, Object> props = new HashMap<>();

        props.put("topic.appendix.field", "TopicRoute");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("TopicRoute", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("TopicRoute", "");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("TopicRoute").schema());
        assertEquals("", ((Struct) transformedRecord.value()).getString("TopicRoute").toString());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("TopicRoute").schema());
        assertEquals("test-undefined", transformedRecord.topic());
        assertNotNull(((Struct) transformedRecord.value()).getString("TopicRoute"));

        // Exercise caching
        final SourceRecord transformedRecord2 = xform.apply(
                new SourceRecord(null, null, "test-undefined", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }



    @Test
    public void schemalessTopicRouterFieldFilled() {
        final Map<String, Object> props = new HashMap<>();

        props.put("topic.appendix.field", "TopicRoute");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("TopicRoute", "appendix"));

        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals("appendix", ((Map) transformedRecord.value()).get("TopicRoute"));
        assertNotNull(((Map) transformedRecord.value()).get("TopicRoute"));
        assertEquals("test-appendix", transformedRecord.topic());

    }

    @Test
    public void schemalessTopicRouterFieldEmpty() {
        final Map<String, Object> props = new HashMap<>();

        props.put("topic.appendix.field", "TopicRoute");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("TopicRoute", ""));

        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals("", ((Map) transformedRecord.value()).get("TopicRoute"));
        assertNotNull(((Map) transformedRecord.value()).get("TopicRoute"));
        assertEquals("test-undefined", transformedRecord.topic());
    }

    @Test
    public void schemalessTopicRouterFieldMissing() {
        final Map<String, Object> props = new HashMap<>();

        props.put("field", "foo");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("field", "foo"));

        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals("test", transformedRecord.topic());

    }
}