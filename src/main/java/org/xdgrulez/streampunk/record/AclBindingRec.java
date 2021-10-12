package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class AclBindingRec {
    private ResourcePatternRec resourcePatternRec;
    private AccessControlEntryRec accessControlEntryRec;

    public ResourcePatternRec getResourcePattern() {
        return resourcePatternRec;
    }
    public void setResourcePattern(ResourcePatternRec resourcePatternRec) {
        this.resourcePatternRec = resourcePatternRec;
    }

    public AccessControlEntryRec getAccessControlEntry() {
        return accessControlEntryRec;
    }
    public void setAccessControlEntry(AccessControlEntryRec accessControlEntryRec) {
        this.accessControlEntryRec = accessControlEntryRec;
    }

    public AclBindingRec(ResourcePatternRec resourcePatternRec, AccessControlEntryRec accessControlEntryRec) {
        this.resourcePatternRec = resourcePatternRec;
        this.accessControlEntryRec = accessControlEntryRec;
    }

    public AclBindingRec(AclBinding aclBinding) {
        this.resourcePatternRec = new ResourcePatternRec(aclBinding.pattern());
        this.accessControlEntryRec = new AccessControlEntryRec(aclBinding.entry());
    }

    public AclBinding aclBinding() {
        ResourcePattern resourcePattern =
                new ResourcePattern(
                        ResourceType.fromString(this.resourcePatternRec.getResourceType()),
                        this.resourcePatternRec.getName(),
                        PatternType.fromString(this.resourcePatternRec.getPatternType()));
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(
                        this.accessControlEntryRec.getPrincipal(),
                        this.accessControlEntryRec.getHost(),
                        AclOperation.fromString(this.accessControlEntryRec.getOperation()),
                        AclPermissionType.fromString(this.accessControlEntryRec.getPermissionType()));
        return new AclBinding(resourcePattern, accessControlEntry);
    }
}
