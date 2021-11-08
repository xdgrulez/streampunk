package org.xdgrulez.streampunk.test;

import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.xdgrulez.streampunk.addon.*;
import org.xdgrulez.streampunk.admin.Cluster;
import org.xdgrulez.streampunk.admin.Group;
import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerByteArray;
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

//    @Test
    public void testEuProdListTopics() {
        var clusterString = "eu-prod";
        //
        var topicStringList = Topic.list(clusterString);
        var filteredTopicStringList =
                topicStringList.stream()
                .filter(topicString ->
                        !topicString.contains("KSTREAM")
                                                && !topicString.contains("KTABLE")
                                                && !topicString.contains("repartition")
                                                && !topicString.contains("changelog")
                                                && !topicString.startsWith("connect-")
                                                && !topicString.startsWith("_")
                                                && !topicString.startsWith("dlq-lcc"))
                        .collect(Collectors.toList());
        var filteredTopicString = String.join(",", filteredTopicStringList);
        System.out.println(filteredTopicString);
    }

//    @Test
    public void testAllProdTopics() {
        var prodClusterString = "eu-prod";
        //
        var prodTopicStringList = Topic.list(prodClusterString);
        //
        var atomicInteger = new AtomicInteger(1);
        //
        var diyBeginInt = 1;
        prodTopicStringList
                .stream()
                .filter(topicString -> topicString.contains(".diy."))
                .forEach(topicString -> {
                    var partitionsInt = Topic.getPartitions(prodClusterString, topicString);
                    System.out.printf("DIY,%s,%d\n", topicString, partitionsInt);
                    atomicInteger.incrementAndGet();
                });
        var diyEndInt = atomicInteger.get() - 1;
        //
        var pro360BeginInt = atomicInteger.get();
        prodTopicStringList
                .stream()
                .filter(topicString -> topicString.contains(".assetmgmt.") || topicString.contains(".customermgmt.") || topicString.contains(".productmgmt.") || topicString.contains(".guaranteemgmt.") || topicString.contains("gsp."))
                .forEach(topicString -> {
                    var partitionsInt = Topic.getPartitions(prodClusterString, topicString);
                    System.out.printf("Pro360/GSP,%s,%d\n", topicString, partitionsInt);
                    atomicInteger.incrementAndGet();
                });
        var pro360EndInt = atomicInteger.get() - 1;
        //
        var ddBeginInt = atomicInteger.get();
        prodTopicStringList
                .stream()
                .filter(topicString -> topicString.contains(".manufacturing.pt") || topicString.contains(".streamingetl"))
                .forEach(topicString -> {
                    var partitionsInt = Topic.getPartitions(prodClusterString, topicString);
                    System.out.printf("DD,%s,%d\n", topicString, partitionsInt);
                    atomicInteger.incrementAndGet();
                });
        var ddEndInt = atomicInteger.get() - 1;
        //
        var restBeginInt = atomicInteger.get();
        prodTopicStringList
                .stream()
                .filter(topicString ->
                        !(topicString.contains(".diy.") || topicString.contains(".assetmgmt.") || topicString.contains(".customermgmt.") || topicString.contains(".productmgmt.") || topicString.contains(".guaranteemgmt.") || topicString.contains("gsp.") || (topicString.contains(".manufacturing.pt") || topicString.contains(".streamingetl"))))
                .forEach(topicString -> {
                    var partitionsInt = Topic.getPartitions(prodClusterString, topicString);
                    System.out.printf("rest,%s,%d\n", topicString, partitionsInt);
                    atomicInteger.incrementAndGet();
                });
        var restEndInt = atomicInteger.get() - 1;
        //
        System.out.printf("\nDIY total,,=sum(c%d:c%d)\n", diyBeginInt, diyEndInt);
        System.out.printf("Pro360/GSP total,,=sum(c%d:c%d)\n", pro360BeginInt, pro360EndInt);
        System.out.printf("DD total,,=sum(c%d:c%d)\n", ddBeginInt, ddEndInt);
        System.out.printf("rest total,,=sum(c%d:c%d)\n", restBeginInt, restEndInt);
        System.out.printf("\ntotal,,=sum(c%d:c%d)\n", diyBeginInt, restEndInt);
    }

//    @Test
    public void testAllDevTopics() {
        var devClusterString = "eu-dev";
        //
        var devTopicStringList = Topic.list(devClusterString);
        //
        var localSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("local") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("local,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var sandboxSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("sandbox") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("sandbox,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var sboxSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("sbox") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("sbox,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var devSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("dev") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("dev,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var qaSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("qa") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("qa,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var reviewSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("review") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("review,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var intSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> topicString.startsWith("int") &&
                        (topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("int,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);
        //
        var restSumPartitionsInteger = devTopicStringList
                .stream()
                .filter(topicString -> !(topicString.contains("assetmgmt.") || topicString.contains("customermgmt.") || topicString.contains("productmgmt.") || topicString.contains("guaranteemgmt.") || topicString.contains("gsp.")))
                .map(topicString -> {
                    var partitionsInt = Topic.getPartitions(devClusterString, topicString);
                    System.out.printf("rest,%s,%d\n", topicString, partitionsInt);
                    return partitionsInt;
                })
                .reduce(0,
                        (accPartitionsInt, partitionsInt) ->
                                accPartitionsInt += partitionsInt);

        //
        System.out.println();
        //
        System.out.printf("Pro360/GSP local,,%d\n", localSumPartitionsInteger);
        System.out.printf("Pro360/GSP sandbox,,%d\n", sandboxSumPartitionsInteger);
        System.out.printf("Pro360/GSP sbox,,%d\n", sboxSumPartitionsInteger);
        System.out.printf("Pro360/GSP dev,,%d\n", devSumPartitionsInteger);
        System.out.printf("Pro360/GSP qa,,%d\n", qaSumPartitionsInteger);
        System.out.printf("Pro360/GSP review,,%d\n", reviewSumPartitionsInteger);
        System.out.printf("Pro360/GSP int,,%d\n", intSumPartitionsInteger);
        System.out.printf("Pro360/GSP total,,%d\n", localSumPartitionsInteger + sandboxSumPartitionsInteger + sboxSumPartitionsInteger + devSumPartitionsInteger + qaSumPartitionsInteger + reviewSumPartitionsInteger + intSumPartitionsInteger);
        //
        System.out.println();
        //
        System.out.printf("rest (DD, DIY, BECO...),,%d\n", restSumPartitionsInteger);
        //
        System.out.printf("\ntotal (all projects),,%d\n", localSumPartitionsInteger + sandboxSumPartitionsInteger + sboxSumPartitionsInteger + devSumPartitionsInteger + qaSumPartitionsInteger + reviewSumPartitionsInteger + intSumPartitionsInteger + restSumPartitionsInteger);
    }

//    @Test
    public void testConsumeTopic() throws InterruptedException {
        var clusterString = "eu-dev";
        var topicString = "int.assetmgmt.registry.companies-v2";
        //
        var x = ConsumerByteArray.consumeN(clusterString, topicString, "test", 1, 9, 57216L, false, 1);
        System.out.println(x.get(0).value()[0]);
    }

    //    @Test
    public void findAllSalesPackagingDmc() {
        var clusterString = "eu-prod";
        var topicString = "prod.devices.manufacturing.pt.dd.v3";
        //
        var offsetsRec = Topic.getOffsets(clusterString, topicString);
        //
        Pred<ConsumerRecord<String, DynamicMessage>> findAllPred = consumerRecord -> {
            var salesPackagingDmcString =
                    Helpers.getProtobufField(consumerRecord.value(), "SalesPackaging", "Dmc");
            //
            return salesPackagingDmcString.equals("01031651408426931120070921027073455240797");
        };
        //
        var partitionsInt = Topic.getPartitions(clusterString, topicString);
        var startOffsets = Helpers.getZeroOffsets(partitionsInt);
        var endOffsets = Helpers.getOffsets(partitionsInt, 2000000);
        //
        var consumerRecordList =
                Lookup.findAllStringProtobuf(clusterString, topicString, findAllPred,
                        startOffsets, endOffsets);
        //
        for (var consumerRecord: consumerRecordList) {
            System.out.println("---");
            System.out.printf("Found in topic: %s, partition: %d, offset: %d\n",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            System.out.println(consumerRecord.key());
            System.out.println(consumerRecord.value());
            System.out.println("---");
        }
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
