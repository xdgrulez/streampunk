package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

public class ResourcePatternFilterRec {
    private String resourceTypeString;
    private String nameString;
    private String patternTypeString;

    public String getResourceType() {
        return this.resourceTypeString;
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

    public ResourcePatternFilterRec(String resourceTypeString, String nameString, String patternTypeString) {
        this.resourceTypeString = resourceTypeString;
        this.nameString = nameString;
        this.patternTypeString = patternTypeString;
    }

    public ResourcePatternFilterRec(ResourcePatternFilter resourcePatternFilter) {
        this.resourceTypeString = resourcePatternFilter.resourceType().name();
        this.nameString = resourcePatternFilter.name();
        this.patternTypeString = resourcePatternFilter.patternType().name();
    }

    public ResourcePatternFilter resourcePatternFilter() {
        return new ResourcePatternFilter(
                ResourceType.fromString(this.resourceTypeString),
                this.nameString,
                PatternType.fromString(this.patternTypeString));
    }
}
