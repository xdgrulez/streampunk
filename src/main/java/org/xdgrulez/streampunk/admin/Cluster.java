package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import org.xdgrulez.streampunk.record.ConfigRec;
import org.xdgrulez.streampunk.record.ConfigResourceRec;
import org.xdgrulez.streampunk.record.DescribeClusterResultRec;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Cluster {
    public static List<String> list() {
        var fileStringList = Helpers.listFiles("./clusters", ".*\\.properties");
        return fileStringList
                .stream()
                .map(fileString -> fileString.replaceAll("\\.properties", ""))
                .collect(Collectors.toList());
    }

    public static void setConfig(String clusterString, Map<String, String> configStringStringMap) {
        var configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        var configRec = new ConfigRec(configResource, configStringStringMap);
        Admin.alterConfig(clusterString, configRec);
    }

    public static Map<String, String> getConfig(String clusterString) {
        var configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        var configResourceRec = new ConfigResourceRec(configResource);
        return Admin.describeConfig(clusterString, configResourceRec);
    }

    public static DescribeClusterResultRec describe(String clusterString) {
        var adminClient = AdminClientEnum.INSTANCE.get(clusterString);

        var describeClusterResult = adminClient.describeCluster();
        //
        DescribeClusterResultRec describeClusterResultRec;
        try {
            describeClusterResultRec = new DescribeClusterResultRec(
                    describeClusterResult.authorizedOperations().get(),
                    describeClusterResult.clusterId().get(),
                    describeClusterResult.controller().get(),
                    describeClusterResult.nodes().get());
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        return describeClusterResultRec;
    }
}
