package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.record.ConfigRec;
import org.xdgrulez.streampunk.record.ConfigResourceRec;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Admin {
    public static void alterConfig(String clusterString, ConfigRec configRec) {
        var config = configRec.config();
        var configResourceAlterConfigOpCollectionMap = new HashMap<ConfigResource, Collection<AlterConfigOp>>();
        configResourceAlterConfigOpCollectionMap.put(config.getKey(), config.getValue());
        //
        var alterConfigsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .incrementalAlterConfigs(configResourceAlterConfigOpCollectionMap);
        //
        try {
            alterConfigsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
    }

    public static Map<String, String> describeConfig(String clusterString, ConfigResourceRec configResourceRec) {
        var configResource = configResourceRec.configResource();
        //
        var describeConfigsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .describeConfigs(Collections.singletonList(configResource));

        //
        Map<ConfigResource, Config> configResourceConfigMap;
        try {
            configResourceConfigMap = describeConfigsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        //
        return configResourceConfigMap
                .get(configResource)
                .entries()
                .stream()
                .filter(configEntry -> configEntry.value() != null)
                .map(configEntry -> Map.entry(configEntry.name(), configEntry.value()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
