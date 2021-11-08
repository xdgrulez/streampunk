package org.xdgrulez.streampunk.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.xdgrulez.streampunk.addon.Replicate;
import org.xdgrulez.streampunk.admin.Group;
import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerString;
import org.xdgrulez.streampunk.producer.ProducerString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicateTest {

    //@Test
    public void testReplicateTopic() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        if (Topic.exists("local", "testtopic2")) {
            Topic.delete("local", "testtopic2", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic2", 1, 1, null);
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        sourceTopicStringTargetTopicStringMap.put("testtopic2", "testtopic2.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Check whether the replicated topics exist
        assertTrue(Topic.exists("local", "testtopic1.copy"));
        assertTrue(Topic.exists("local", "testtopic2.copy"));
    }

    //@Test
    public void testReplicateTopicContent() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        if (Topic.exists("local", "testtopic2")) {
            Topic.delete("local", "testtopic2", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic2", 1, 1, null);
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        ProducerString.produce("local", "testtopic1", "record1_2");
        ProducerString.produce("local", "testtopic1", "record1_3");
        //
        ProducerString.produce("local", "testtopic2", "record2_1");
        ProducerString.produce("local", "testtopic2", "record2_2");
        ProducerString.produce("local", "testtopic2", "record2_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        sourceTopicStringTargetTopicStringMap.put("testtopic2", "testtopic2.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the content of the test topic is correctly replicated
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "testtopic1.copy.group",
                        3, 0, 0L, false, 1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        var consumerRecordList2 =
                ConsumerString.consumeN("local", "testtopic2.copy", "testtopic2.copy.group",
                        3, 0, 0L, false, 1);
        assertEquals("record2_1", consumerRecordList2.get(0).value());
        assertEquals("record2_2", consumerRecordList2.get(1).value());
        assertEquals("record2_3", consumerRecordList2.get(2).value());
    }

//    @Test
    public void testReplicateTopicContentTargetTopicsDifferentNumberOfPartitions() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 2, 1, null);
        //
        if (Topic.exists("local", "testtopic2")) {
            Topic.delete("local", "testtopic2", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic2", 2, 1, null);
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "key1", "record1_1");
        ProducerString.produce("local", "testtopic1", "key2", "record1_2");
        ProducerString.produce("local", "testtopic1", "key3", "record1_3");
        //
        ProducerString.produce("local", "testtopic2", "key1", "record2_1");
        ProducerString.produce("local", "testtopic2", "key2", "record2_2");
        ProducerString.produce("local", "testtopic2", "key3", "record2_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap =
                Map.of("testtopic1", "testtopic1.copy", "testtopic2", "testtopic2.copy");
        var sourceTopicStringTargetPartitionsIntMap =
                Map.of("testtopic1", 1, "testtopic2", 3);
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                sourceTopicStringTargetPartitionsIntMap,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the content of the test topic is correctly replicated
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "testtopic1.copy.group",
                        3, 0, 0L, false, 1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        //
        var consumerRecordList22 =
                ConsumerString.consumeN("local", "testtopic2.copy", "testtopic2.copy.group",
                        2, 2, 0L, false, 1);
        assertEquals("record2_1", consumerRecordList22.get(0).value());
        assertEquals("record2_2", consumerRecordList22.get(1).value());
        var consumerRecordList21 =
                ConsumerString.consumeN("local", "testtopic2.copy", "testtopic2.copy.group",
                        1, 1, 0L, false, 1);
        assertEquals("record2_3", consumerRecordList21.get(0).value());
    }

    //@Test
    public void testReplicateConsumerGroups() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        if (Topic.exists("local", "testtopic2")) {
            Topic.delete("local", "testtopic2", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic2", 1, 1, null);
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_3");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic2", "record2_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic2", "record2_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic2", "record2_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        sourceTopicStringTargetTopicStringMap.put("testtopic2", "testtopic2.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        var consumerRecordList2 =
                ConsumerString.consumeN("local", "testtopic2.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList2);
        assertEquals("record2_1", consumerRecordList2.get(0).value());
        assertEquals("record2_2", consumerRecordList2.get(1).value());
        assertEquals("record2_3", consumerRecordList2.get(2).value());
        // Create test consumer groups
        ConsumerString.consumeN("local", "testtopic1", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group3",
                1, 0, 2L, false, 1);
        //
        ConsumerString.consumeN("local", "testtopic2", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic2", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic2", "group3",
                1, 0, 2L, false, 1);
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1").get(0));
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic2").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic2").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic2").get(0));
        // Replicate the consumer groups
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                null);
        // Check whether the consumer groups are correctly replicated
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1.copy").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1.copy").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1.copy").get(0));
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic2.copy").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic2.copy").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic2.copy").get(0));
    }

    //@Test
    public void testReplicateConsumerGroupsEmptyTopic() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        // Replicate the test topic
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Replicate the consumer groups
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                null);
    }

    //@Test
    public void testReplicateConsumerGroupsWhiteList() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        // Create test consumer groups
        ConsumerString.consumeN("local", "testtopic1", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group3",
                1, 0, 2L, false, 1);
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1").get(0));
        // Replicate the consumer groups
        var includeGroupStringList = new ArrayList<String>();
        includeGroupStringList.add("group1");
        includeGroupStringList.add("group2");
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                includeGroupStringList,
                null,
                null,
                null);
        // Check whether the consumer groups are correctly replicated
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1.copy").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1.copy").get(0));
        assertEquals(null,
                Group.getOffsets("local", "group3").get("testtopic1.copy"));
    }

    //@Test
    public void testReplicateConsumerGroupsWhiteRegexp() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        // Create test consumer groups
        ConsumerString.consumeN("local", "testtopic1", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group3",
                1, 0, 2L, false, 1);
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1").get(0));
        // Replicate the consumer groups
        var includeGroupStringList = new ArrayList<String>();
        includeGroupStringList.add("group1");
        includeGroupStringList.add("group2");
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                "group[1-2]",
                null,
                null);
        // Check whether the consumer groups are correctly replicated
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1.copy").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1.copy").get(0));
        assertEquals(null,
                Group.getOffsets("local", "group3").get("testtopic1.copy"));
    }

    //@Test
    public void testReplicateConsumerGroupsBlackList() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic1", "record1_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        // Create test consumer groups
        ConsumerString.consumeN("local", "testtopic1", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group3",
                1, 0, 2L, false, 1);
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1").get(0));
        // Replicate the consumer groups
        var excludeGroupStringList = new ArrayList<String>();
        excludeGroupStringList.add("group3");
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                excludeGroupStringList,
                null);
        var x = Group.getOffsets("local", "group1");
        // Check whether the consumer groups are correctly replicated
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1.copy").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1.copy").get(0));
        assertEquals(null,
                Group.getOffsets("local", "group3").get("testtopic1.copy"));
    }

    //@Test
    public void testReplicateConsumerGroupsBlackRegexp() throws InterruptedException {
        // Create test topics
        if (Topic.exists("local", "testtopic1")) {
            Topic.delete("local", "testtopic1", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic1", 1, 1, null);
        //
        // Produce data into the test topics
        ProducerString.produce("local", "testtopic1", "record1_1");
        ProducerString.produce("local", "testtopic1", "record1_2");
        ProducerString.produce("local", "testtopic1", "record1_3");
        // Replicate the test topics
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic1", "testtopic1.copy");
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                true);
        // Replicate the content of the test topics
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList1 =
                ConsumerString.consumeN("local", "testtopic1.copy", "group1.copy",
                        3, 0, 0L, false, 1);
//        System.out.println(consumerRecordList1);
        assertEquals("record1_1", consumerRecordList1.get(0).value());
        assertEquals("record1_2", consumerRecordList1.get(1).value());
        assertEquals("record1_3", consumerRecordList1.get(2).value());
        // Create test consumer groups
        ConsumerString.consumeN("local", "testtopic1", "group1",
                1, 0, 0L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group2",
                1, 0, 1L, false, 1);
        ConsumerString.consumeN("local", "testtopic1", "group3",
                1, 0, 2L, false, 1);
        //
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1").get(0));
        assertEquals(2,
                Group.getOffsets("local", "group2").get("testtopic1").get(0));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1").get(0));
        // Replicate the consumer groups
        var excludeGroupRegexpString = ".*oup2";
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                null,
                null,
                null,
                excludeGroupRegexpString);
        // Check whether the consumer groups are correctly replicated
        assertEquals(1,
                Group.getOffsets("local", "group1").get("testtopic1.copy").get(0));
        assertEquals(null,
                Group.getOffsets("local", "group2").get("testtopic1.copy"));
        assertEquals(3,
                Group.getOffsets("local", "group3").get("testtopic1.copy").get(0));
    }

    //@Test
    public void testReplicateAndJoinConsumerGroups1() throws InterruptedException {
        //                *
        // r1_1 r1_2 r1_3
        // r2_1 r2_2 r2_3
        //           *
        //
        //                   *X
        // => r1_1 r1_2 r1_3 r2_1 r2_2 r2_3
        //                             *!
        // Why? We can set the offset to the offset on partition 2 (record: r2_3) because
        // the offset on partition 1 had lag = 0/was at the end of partition 1.
        // Create test topic
        if (Topic.exists("local", "testtopic")) {
            Topic.delete("local", "testtopic", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic", 2, 1, null);
        // Produce data into the test topic
        ProducerString.produce("local", "testtopic", 0, null, "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_3");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_3");
        // Replicate the test topic
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic", "testtopic.copy");
        var sourceTopicStringTargetPartitionsIntMap = new HashMap<String, Integer>();
        sourceTopicStringTargetPartitionsIntMap.put("testtopic", 1);
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                sourceTopicStringTargetPartitionsIntMap,
                null,
                true);
        // Replicate the content of the test topics
        var sourceTopicStringPartitionIntTargetPartitionIntMapMap = new HashMap<String, Map<Integer, Integer>>();
        var sourcePartitionIntTargetPartitionIntMap = new HashMap<Integer, Integer>();
        sourcePartitionIntTargetPartitionIntMap.put(0, 0);
        sourcePartitionIntTargetPartitionIntMap.put(1, 0);
        sourceTopicStringPartitionIntTargetPartitionIntMapMap.put("testtopic", sourcePartitionIntTargetPartitionIntMap);
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList =
                ConsumerString.consumeN("local", "testtopic.copy", "group.copy",
                        6, 0, 0L, false, 1);
        var recordStringList = consumerRecordList.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        assertTrue(recordStringList.contains("record1_1"));
        assertTrue(recordStringList.contains("record1_2"));
        assertTrue(recordStringList.contains("record1_3"));
        assertTrue(recordStringList.contains("record2_1"));
        assertTrue(recordStringList.contains("record2_2"));
        assertTrue(recordStringList.contains("record2_3"));
        // Create test consumer group
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 0, 2L, false, 1).get(0).timestamp();
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 1, 1L, false, 1).get(0).timestamp();
        // Check the offsets of the test consumer group
        assertEquals(3, Group.getOffsets("local", "group").get("testtopic").get(0));
        assertEquals(2, Group.getOffsets("local", "group").get("testtopic").get(1));
        // Replicate the consumer groups
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                null);
        // Check whether the consumer group is correctly replicated
        var targetValueString =
                ConsumerString.consumeN("local", "testtopic.copy", "group",
                        1, 0, null, false, 1).get(0).value();
        assertEquals("record2_3", targetValueString);
    }

    //@Test
    public void testReplicateAndJoinConsumerGroups2() throws InterruptedException {
        //           *
        // r1_1 r1_2 r1_3
        // r2_1 r2_2 r2_3
        //           *
        //
        //              *!
        // => r1_1 r1_2 r1_3 r2_1 r2_2 r2_3
        //                             *X
        // Why? We have to set the offset to the offset on partition 1 (record: r1_3) because
        // the offset on partition 1 does not have lag = 0/is not at the end of partition 1 (reprocessing involved).
        // Create test topic
        if (Topic.exists("local", "testtopic")) {
            Topic.delete("local", "testtopic", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic", 2, 1, null);
        // Produce data into the test topic
        ProducerString.produce("local", "testtopic", 0, null, "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_3");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_3");
        // Replicate the test topic
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic", "testtopic.copy");
        var sourceTopicStringTargetPartitionsIntMap = new HashMap<String, Integer>();
        sourceTopicStringTargetPartitionsIntMap.put("testtopic", 1);
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                sourceTopicStringTargetPartitionsIntMap,
                null,
                true);
        // Replicate the content of the test topics
        var sourceTopicStringPartitionIntTargetPartitionIntMapMap = new HashMap<String, Map<Integer, Integer>>();
        var sourcePartitionIntTargetPartitionIntMap = new HashMap<Integer, Integer>();
        sourcePartitionIntTargetPartitionIntMap.put(0, 0);
        sourcePartitionIntTargetPartitionIntMap.put(1, 0);
        sourceTopicStringPartitionIntTargetPartitionIntMapMap.put("testtopic", sourcePartitionIntTargetPartitionIntMap);
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        var consumerRecordList =
                ConsumerString.consumeN("local", "testtopic.copy", "group.copy",
                        6, 0, 0L, false, 1);
        List<String> recordStringList = consumerRecordList.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        assertTrue(recordStringList.contains("record1_1"));
        assertTrue(recordStringList.contains("record1_2"));
        assertTrue(recordStringList.contains("record1_3"));
        assertTrue(recordStringList.contains("record2_1"));
        assertTrue(recordStringList.contains("record2_2"));
        assertTrue(recordStringList.contains("record2_3"));
        // Create test consumer group
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 0, 1L, false, 1).get(0).timestamp();
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 1, 2L, false, 1).get(0).timestamp();
        // Check the offsets of the test consumer group
        assertEquals(2, Group.getOffsets("local", "group").get("testtopic").get(0));
        assertEquals(3, Group.getOffsets("local", "group").get("testtopic").get(1));
        // Replicate the consumer groups
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                null);
        // Check whether the consumer group is correctly replicated
        String targetValueString =
                ConsumerString.consumeN("local", "testtopic.copy", "group",
                        1, 0, null, false, 1).get(0).value();
        assertEquals("record1_3", targetValueString);
    }

    //@Test
    public void testReplicateAndJoinConsumerGroups3() throws InterruptedException {
        //      *
        // r1_1 r1_2 r1_3
        // r2_1 r2_2 r2_3
        //      *
        //
        //         *!
        // => r1_1 r1_2 r1_3 r2_1 r2_2 r2_3
        //                        *X
        // Why? We have to set the offset to the offset on partition 1 (record: r1_1) because
        // the offset on partition 1 does not have lag = 0/is not at the end of partition 1 (reprocessing involved).
        // Create test topic
        if (Topic.exists("local", "testtopic")) {
            Topic.delete("local", "testtopic", false);
            Thread.sleep(1000);
        }
        Topic.create("local", "testtopic",
                2, 1, null);
        // Produce data into the test topic
        ProducerString.produce("local", "testtopic", 0, null, "record1_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 0, null, "record1_3");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_1");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_2");
        Thread.sleep(10);
        ProducerString.produce("local", "testtopic", 1, null, "record2_3");
        // Replicate the test topic
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put("testtopic", "testtopic.copy");
        var sourceTopicStringTargetPartitionsIntMap = new HashMap<String, Integer>();
        sourceTopicStringTargetPartitionsIntMap.put("testtopic", 1);
        Replicate.replicateTopics("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                null,
                sourceTopicStringTargetPartitionsIntMap,
                null,
                true);
        // Replicate the content of the test topics
        var sourceTopicStringPartitionIntTargetPartitionIntMapMap = new HashMap<String, Map<Integer, Integer>>();
        var sourcePartitionIntTargetPartitionIntMap = new HashMap<Integer, Integer>();
        sourcePartitionIntTargetPartitionIntMap.put(0, 0);
        sourcePartitionIntTargetPartitionIntMap.put(1, 0);
        sourceTopicStringPartitionIntTargetPartitionIntMapMap.put("testtopic", sourcePartitionIntTargetPartitionIntMap);
        Replicate.replicateTopicContents("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                false,
                true,
                true);
        // Check whether the replicated content of the test topic exists
        List<ConsumerRecord<String, String>> consumerRecordList =
                ConsumerString.consumeN("local", "testtopic.copy", "group.copy",
                        6, 0, 0L, false, 1);
        List<String> recordStringList = consumerRecordList.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        assertTrue(recordStringList.contains("record1_1"));
        assertTrue(recordStringList.contains("record1_2"));
        assertTrue(recordStringList.contains("record1_3"));
        assertTrue(recordStringList.contains("record2_1"));
        assertTrue(recordStringList.contains("record2_2"));
        assertTrue(recordStringList.contains("record2_3"));
        // Create test consumer group
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 0, 0L, false, 1).get(0).timestamp();
        ConsumerString.consumeN("local", "testtopic", "group",
                1, 1, 0L, false, 1).get(0).timestamp();
        // Check the offsets of the test consumer group
        assertEquals(1, Group.getOffsets("local", "group").get("testtopic").get(0));
        assertEquals(1, Group.getOffsets("local", "group").get("testtopic").get(1));
        // Replicate the consumer groups
        Replicate.replicateConsumerGroups("local",
                "local",
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                null,
                null,
                null,
                null);
        // Check whether the consumer group is correctly replicated
        String targetValueString =
                ConsumerString.consumeN("local", "testtopic.copy", "group",
                        1, 0, null, false, 1).get(0).value();
        assertEquals("record1_2", targetValueString);
    }
}
