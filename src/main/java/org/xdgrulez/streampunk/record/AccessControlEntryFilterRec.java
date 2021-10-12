package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

public class AccessControlEntryFilterRec {
    String principalString;
    String hostString;
    String operationString;
    String permissionTypeString;

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

    public AccessControlEntryFilterRec(String principalString,
                                       String hostString,
                                       String operationString,
                                       String permissionTypeString) {
        this.principalString = principalString;
        this.hostString = hostString;
        this.operationString = operationString;
        this.permissionTypeString = permissionTypeString;
    }

    public AccessControlEntryFilterRec(AccessControlEntryFilter accessControlEntryFilter) {
        this.principalString = accessControlEntryFilter.principal();
        this.hostString = accessControlEntryFilter.host();
        this.operationString = accessControlEntryFilter.operation().name();
        this.permissionTypeString = accessControlEntryFilter.permissionType().name();
    }

    public AccessControlEntryFilter accessControlEntryFilter() {
        return new AccessControlEntryFilter(
                this.principalString,
                this.hostString,
                AclOperation.fromString(this.operationString),
                AclPermissionType.fromString(this.permissionTypeString));
    }
}
