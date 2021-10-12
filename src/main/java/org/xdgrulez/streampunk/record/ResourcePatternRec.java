package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class ResourcePatternRec {
    private String resourceTypeString;
    private String nameString;
    private String patternTypeString;

    public String getResourceType() {
        return resourceTypeString;
    }
    public void setResourceType(String resourceTypeString) {
        this.resourceTypeString = resourceTypeString;
    }

    public String getName() {
        return this.nameString;
    }
    public void setName(String nameString) {
        this.nameString = nameString;
    }

    public String getPatternType() {
        return this.patternTypeString;
    }
    public void setPatternType(String patternTypeString) {
        this.patternTypeString = patternTypeString;
    }

    public ResourcePatternRec(String resourceTypeString, String nameString, String patternTypeString) {
        this.resourceTypeString = resourceTypeString;
        this.nameString = nameString;
        this.patternTypeString = patternTypeString;
    }

    public ResourcePatternRec(ResourcePattern resourcePattern) {
        this.resourceTypeString = resourcePattern.resourceType().name();
        this.nameString = resourcePattern.name();
        this.patternTypeString = resourcePattern.patternType().name();
    }

    public ResourcePattern resourcePattern() {
        return new ResourcePattern(
                ResourceType.fromString(this.resourceTypeString),
                this.nameString,
                PatternType.fromString(this.patternTypeString));
    }
}
