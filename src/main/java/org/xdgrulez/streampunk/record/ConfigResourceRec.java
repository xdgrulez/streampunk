package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.config.ConfigResource;

public class ConfigResourceRec {
    private String typeString;
    private String nameString;

    public String getType() {
        return typeString;
    }
    public void setType(String typeString) {
        this.typeString = typeString;
    }

    public String getName() {
        return nameString;
    }
    public void setName(String nameString) {
        this.nameString = nameString;
    }

    public ConfigResourceRec(String typeString, String nameString) {
        this.typeString = typeString;
        this.nameString = nameString;
    }

    public ConfigResourceRec(ConfigResource configResource) {
        this.typeString = configResource.type().name();
        this.nameString = configResource.name();
    }

    public ConfigResource configResource() {
        return new ConfigResource(ConfigResource.Type.valueOf(this.typeString), this.nameString);
    }
}
