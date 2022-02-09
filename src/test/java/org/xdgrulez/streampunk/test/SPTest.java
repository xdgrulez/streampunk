package org.xdgrulez.streampunk.test;

import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.xdgrulez.streampunk.addon.*;
import org.xdgrulez.streampunk.admin.Cluster;
import org.xdgrulez.streampunk.admin.Group;
import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerByteArray;
import org.xdgrulez.streampunk.consumer.ConsumerString;
import org.xdgrulez.streampunk.consumer.ConsumerStringAvro;
import org.xdgrulez.streampunk.consumer.ConsumerStringProtobuf;
import org.xdgrulez.streampunk.helper.Helpers;
import org.xdgrulez.streampunk.helper.fun.Fun;
import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.producer.ProducerStringAvro;
import org.xdgrulez.streampunk.producer.ProducerStringProtobuf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SPTest {

    @BeforeEach
    public void setup() {
    }

    @AfterEach
    public void tearDown() {
    }

    //        @Test
    public void testFixSchema() throws InterruptedException {
        if (Topic.exists("local", "testtopic")) {
            Topic.delete("local", "testtopic", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic", 1, 1, null);
        var schemaString = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"testrecord\",\n" +
                "  \"fields\": [\n" +
                "      {\"name\": \"stringfield\",  \"type\": \"string\" }\n" +
                "    , {\"name\": \"floatfield\", \"type\": \"float\" }\n" +
                "  ]\n" +
                "}";
        ProducerStringAvro.produce("local",
                "testtopic",
                null,
                "{\"stringfield\": \"string1\", \"floatfield\": 500.0}",
                schemaString);
        ProducerStringAvro.produce("local",
                "testtopic",
                null,
                "{\"stringfield\": \"string2\", \"floatfield\": 1000.0}",
                schemaString);
        //
        var consumerRecordList1 = ConsumerStringAvro.consumeN("local",
                "testtopic",
                "testgroup",
                1,
                0,
                0L,
                false,
                1);
        var genericRecord1 = consumerRecordList1.get(0).value();
        assertEquals("string1", genericRecord1.get("stringfield").toString());
        assertEquals("500.0", genericRecord1.get("floatfield").toString());
        //
        SchemaRegistry.deleteSchema("local", "testtopic-value", true);
        //
        assertThrows(SerializationException.class, () -> {
            ConsumerStringAvro.consumeN("local",
                    "testtopic",
                    "testgroup",
                    1,
                    0,
                    0L,
                    false,
                    1);
        });
        //
//        Replicate.fixSchemaId("local", "testtopic", schemaString);
        //
        assertEquals(2, Topic.getTotalSize("local", "testtopic"));
        //
        var consumerRecordList2 = ConsumerStringAvro.consumeN("local",
                "testtopic",
                "testgroup",
                1,
                0,
                null,
                false,
                1);
        var genericRecord2 = consumerRecordList2.get(0).value();
        assertEquals("string2", genericRecord2.get("stringfield").toString());
        assertEquals("1000.0", genericRecord2.get("floatfield").toString());
        //
        assertFalse(Topic.exists("local", "testtopic.tmp"));
    }

    //        @Test
    public void testConsume() throws InterruptedException, IOException {
        if (Topic.exists("local", "testtopic")) {
            Topic.delete("local", "testtopic", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic", 1, 1, null);
        var schemaString = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"testrecord\",\n" +
                "  \"fields\": [\n" +
                "      {\"name\": \"stringfield\",  \"type\": \"string\" }\n" +
                "    , {\"name\": \"floatfield\", \"type\": \"float\" }\n" +
                "  ]\n" +
                "}";
        ProducerStringAvro.produce("local",
                "testtopic",
                null,
                "{\"stringfield\": \"string1\", \"floatfield\": 500.0}",
                schemaString);
        ProducerStringAvro.produce("local",
                "testtopic",
                null,
                "{\"stringfield\": \"string2\", \"floatfield\": 1000.0}",
                schemaString);
        //
        var consumerRecordList1 = ConsumerStringAvro.consumeN("local",
                "testtopic",
                "testgroup",
                1,
                0,
                0L,
                false,
                1);
        var genericRecord1 = consumerRecordList1.get(0).value();
        assertEquals("string1", genericRecord1.get("stringfield").toString());
        assertEquals("500.0", genericRecord1.get("floatfield").toString());
        //
        var oldSchemaIdInt = SchemaRegistry.getSchemaId("local", "testtopic-value");
        //
        SchemaRegistry.deleteSchema("local", "testtopic-value", true);
        //
        assertThrows(SerializationException.class, () -> {
            ConsumerStringAvro.consumeN("local",
                    "testtopic",
                    "testgroup",
                    1,
                    0,
                    0L,
                    false,
                    1);
        });
        //
        Replicate.replicateTopic("local",
                "local",
                "testtopic",
                "testtopic.tmp",
                null,
                null,
                null,
                true);
        //
        Replicate.replicateTopicContent("local",
                "local",
                "testtopic",
                "testtopic.tmp",
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        //
        Replicate.replicateConsumerGroup("local",
                "local",
                "testtopic",
                "testtopic.tmp",
                null,
                null,
                null,
                null,
                null);
        //
        assertEquals(2, Topic.getTotalSize("local", "testtopic.tmp"));
        System.out.println(Group.list("local"));
        //
        Replicate.replicateTopic("local",
                "local",
                "testtopic.tmp",
                "testtopic",
                null,
                null,
                null,
                true);
        //
        int newSchemaIdInt = SchemaRegistry.createSchema("local", "testtopic-value", schemaString);
        Fun<ConsumerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> fixSchemaSmtFun =
                consumerRecord -> {
                    var valueBytes = consumerRecord.value();
                    var magicByte = valueBytes[0];
                    //
                    var schemaIdBytes = new byte[4];
                    System.arraycopy(valueBytes, 1, schemaIdBytes, 0, 4);
                    //
                    var dataBytesInt = valueBytes.length - 5;
                    var dataBytes = new byte[dataBytesInt];
                    System.arraycopy(valueBytes, 5, dataBytes, 0, dataBytesInt);
                    //
                    var schemaIdInt = new BigInteger(schemaIdBytes).intValue();
                    assertEquals(oldSchemaIdInt, schemaIdInt);
                    //
                    var destSchemaIdBytes = ByteBuffer.allocate(4).putInt(newSchemaIdInt).array();
                    //
                    byte[] destValueBytes = new byte[valueBytes.length];
                    destValueBytes[0] = magicByte;
                    System.arraycopy(destSchemaIdBytes, 0, destValueBytes, 1, 4);
                    System.arraycopy(dataBytes, 0, destValueBytes, 5, dataBytesInt);
                    //
                    return new ProducerRecord<>(
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.timestamp(),
                            consumerRecord.key(), destValueBytes, consumerRecord.headers());
                };
        Replicate.replicateTopicContent("local",
                "local",
                "testtopic.tmp",
                "testtopic",
                null,
                null,
                null,
                fixSchemaSmtFun,
                false,
                true,
                true);
        //
        Replicate.replicateConsumerGroup("local",
                "local",
                "testtopic.tmp",
                "testtopic",
                null,
                null,
                null,
                null,
                null);
        System.out.println(Group.list("local"));
        //
        assertEquals(2, Topic.getTotalSize("local", "testtopic"));
        //
        var consumerRecordList = ConsumerStringAvro.consumeN("local",
                "testtopic",
                "testgroup",
                1,
                0,
                null,
                false,
                1);
        var genericRecord = consumerRecordList.get(0).value();
        assertEquals("string2", genericRecord.get("stringfield").toString());
        assertEquals("1000.0", genericRecord.get("floatfield").toString());
        //
        Topic.delete("local", "testtopic.tmp", false);
        Thread.sleep(1000);
        assertFalse(Topic.exists("local", "testtopic.tmp"));
    }

    //    @Test
    public void testProtobuf() throws InterruptedException, IOException {
        var clusterString = "local";
        var topicString = "testProtobuf";
        //
        if (Topic.exists(clusterString, topicString)) {
            Topic.delete(clusterString, topicString, false);
            Thread.sleep(1000);
        }
        Topic.create(clusterString, topicString, 1, 1, null);
        //
        var valueDynamicMessageProduce =
                Helpers.jsonStringToDynamicMessage(
//                        "{\"HotelName\": \"Marriott\", \"Description\": \"Marriott description\"}",
                        "{\"HotelName\": \"\", \"Description\": \"Marriott description\"}",
                        "syntax = \"proto3\"; message Hotel { string HotelName = 1 [default = null]; string Description = 2; }");
        ProducerStringProtobuf.produce(clusterString, topicString, "Marriott", valueDynamicMessageProduce);
        var consumerRecordList =
                ConsumerStringProtobuf.consumeN(clusterString, topicString, "test", 1, 0, 0L, false, 1);

        var keyString = consumerRecordList.get(0).key();
        assertEquals("Marriott", keyString);

        var valueDynamicMessageConsume = consumerRecordList.get(0).value();
        System.out.println(valueDynamicMessageConsume);
        var descriptor = valueDynamicMessageConsume.getDescriptorForType();

//        var fieldDescriptorHotelName = descriptor.findFieldByName("HotelName");
//        assertEquals("Marriott", valueDynamicMessageConsume.getField(fieldDescriptorHotelName));

        var fieldDescriptorDescription = descriptor.findFieldByName("Description");
        assertEquals("Marriott description", valueDynamicMessageConsume.getField(fieldDescriptorDescription));
    }
}
