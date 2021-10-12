package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

public class AccessControlEntryRec {
    private String principalString;
    private String hostString;
    private String operationString;
    private String permissionTypeString;

    public String getPrincipal() {
        return principalString;
    }
    public void setPrincipal(String principalString) {
        this.principalString = principalString;
    }

    public String getHost() {
        return hostString;
    }
    public void setHost(String hostString) {
        this.hostString = hostString;
    }

    public String getOperation() {
        return operationString;
    }
    public void setOperation(String operationString) {
        this.operationString = operationString;
    }

    public String getPermissionType() {
        return permissionTypeString;
    }
    public void setPermissionType(String permissionTypeString) {
        this.permissionTypeString = permissionTypeString;
    }

    public AccessControlEntryRec(String principalString,
                                 String hostString,
                                 String operationString,
                                 String permissionTypeString) {
        this.principalString = principalString;
        this.hostString = hostString;
        this.operationString = operationString;
        this.permissionTypeString = permissionTypeString;
    }

    public AccessControlEntryRec(AccessControlEntry accessControlEntry) {
        this.principalString = accessControlEntry.principal();
        this.hostString = accessControlEntry.host();
        this.operationString = accessControlEntry.operation().name();
        this.permissionTypeString = accessControlEntry.permissionType().name();
    }

    public AccessControlEntry getAccessControlEntry() {
        return new AccessControlEntry(
                this.principalString,
                this.hostString,
                AclOperation.fromString(this.operationString),
                AclPermissionType.fromString(this.permissionTypeString));
    }
}
