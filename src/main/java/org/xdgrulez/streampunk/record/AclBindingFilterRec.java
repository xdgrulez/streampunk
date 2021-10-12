package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

public class AclBindingFilterRec {
    private ResourcePatternFilterRec resourcePatternFilterRec;
    private AccessControlEntryFilterRec accessControlEntryFilterRec;

    public ResourcePatternFilterRec getResourcePatternFilter() {
        return resourcePatternFilterRec;
    }
    public void setResourcePatternFilter(ResourcePatternFilterRec resourcePatternFilterRec) {
        this.resourcePatternFilterRec = resourcePatternFilterRec;
    }

    public AccessControlEntryFilterRec getAccessControlEntry() {
        return accessControlEntryFilterRec;
    }
    public void setAccessControlEntryFilter(AccessControlEntryFilterRec accessControlEntryFilterRec) {
        this.accessControlEntryFilterRec = accessControlEntryFilterRec;
    }

    public AclBindingFilterRec(ResourcePatternFilterRec resourcePatternFilterRec,
                               AccessControlEntryFilterRec accessControlEntryFilterRec) {
        this.resourcePatternFilterRec = resourcePatternFilterRec;
        this.accessControlEntryFilterRec = accessControlEntryFilterRec;
    }

    public AclBindingFilterRec(AclBindingFilter aclBindingFilter) {
        this.resourcePatternFilterRec = new ResourcePatternFilterRec(aclBindingFilter.patternFilter());
        this.accessControlEntryFilterRec = new AccessControlEntryFilterRec(aclBindingFilter.entryFilter());
    }

    public AclBindingFilter aclBindingFilter() {
        ResourcePatternFilter resourcePatternFilter =
                new ResourcePatternFilter(
                        ResourceType.fromString(this.resourcePatternFilterRec.getResourceType()),
                        this.resourcePatternFilterRec.getName(),
                        PatternType.fromString(this.resourcePatternFilterRec.getPatternType()));
        AccessControlEntryFilter accessControlEntryFilter =
                new AccessControlEntryFilter(
                        this.accessControlEntryFilterRec.getPrincipal(),
                        this.accessControlEntryFilterRec.getHost(),
                        AclOperation.fromString(this.accessControlEntryFilterRec.getOperation()),
                        AclPermissionType.fromString(this.accessControlEntryFilterRec.getPermissionType()));
        return new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter);
    }
}
