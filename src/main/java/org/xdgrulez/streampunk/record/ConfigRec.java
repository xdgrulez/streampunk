package org.xdgrulez.streampunk.record;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigRec {
    private ConfigResourceRec configResourceRec;
    private Map<String, String> configStringStringMap;

    public ConfigResourceRec getConfigResource() {
        return configResourceRec;
    }
    public void setConfigResource(ConfigResourceRec configResourceRec) {
        this.configResourceRec = configResourceRec;
    }

    public Map<String, String> getConfig() {
        return configStringStringMap;
    }
    public void setConfig(Map<String, String> configStringStringMap) {
        this.configStringStringMap = configStringStringMap;
    }

    public ConfigRec(ConfigResourceRec configResourceRec, Map<String, String> configStringStringMap) {
        this.configResourceRec = configResourceRec;
        this.configStringStringMap = configStringStringMap;
    }

    public ConfigRec(ConfigResource configResource, List<AlterConfigOp> alterConfigOpList) {
        this.configResourceRec = new ConfigResourceRec(configResource);
        this.configStringStringMap =
                alterConfigOpList
                .stream()
                .map(alterConfigOp ->
                        Map.entry(
                                alterConfigOp.configEntry().name(),
                                alterConfigOp.configEntry().value()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public ConfigRec(ConfigResource configResource, Config config) {
        this.configResourceRec = new ConfigResourceRec(configResource);
        this.configStringStringMap = config
                .entries()
                .stream()
                .map(configEntry ->
                                Map.entry(
                                        configEntry.name(),
                                        configEntry.value()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public ConfigRec(ConfigResource configResource, Map<String, String> configStringStringMap) {
        this.configResourceRec = new ConfigResourceRec(configResource);
        this.configStringStringMap = configStringStringMap;
    }

    public Map.Entry<ConfigResource, Collection<AlterConfigOp>> config() {
        var configResource = configResourceRec.configResource();
        var alterConfigOpCollection =
                this.configStringStringMap
                .entrySet()
                .stream()
                .map(configStringStringEntry -> new AlterConfigOp(
                        new ConfigEntry(
                                configStringStringEntry.getKey(),
                                configStringStringEntry.getValue()),
                        AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());
        return Map.entry(configResource, alterConfigOpCollection);
    }
}
