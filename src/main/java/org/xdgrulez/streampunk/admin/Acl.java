package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.record.AclBindingFilterRec;
import org.xdgrulez.streampunk.record.AclBindingRec;
import org.apache.kafka.common.acl.AclBinding;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Acl {
    public static void create(String clusterString, AclBindingRec aclBindingRec) {
        var aclBindingList = Collections.singletonList(aclBindingRec.aclBinding());
        var createAclsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .createAcls(aclBindingList);
        try {
            createAclsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
    }

    public static void delete(String clusterString, AclBindingFilterRec aclBindingFilterRec) {
        var aclBindingFilterList = Collections.singletonList(aclBindingFilterRec.aclBindingFilter());
        var deleteAclsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .deleteAcls(aclBindingFilterList);
        try {
            deleteAclsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
    }

    public static List<AclBindingRec> describe(String clusterString, AclBindingFilterRec aclBindingFilterRec) {
        var describeAclsResult = AdminClientEnum.INSTANCE
                .get(clusterString).describeAcls(aclBindingFilterRec.aclBindingFilter());
        Collection<AclBinding> aclBindingCollection;
        try {
            aclBindingCollection = describeAclsResult.values().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        return aclBindingCollection
                .stream()
                .map(AclBindingRec::new)
                .collect(Collectors.toList());
    }
}
