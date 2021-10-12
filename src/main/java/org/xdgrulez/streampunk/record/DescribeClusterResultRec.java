package org.xdgrulez.streampunk.record;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DescribeClusterResultRec {
    private List<AclOperation> aclOperationList;
    private String clusterIdString;
    private NodeRec controllerNodeRec;
    private List<NodeRec> nodeRecList;

    public List<AclOperation> getAclOperations() {
        return aclOperationList;
    }
    public void setAclOperations(List<AclOperation> aclOperationList) {
        this.aclOperationList = aclOperationList;
    }

    public String getClusterId() {
        return clusterIdString;
    }
    public void setClusterId(String clusterIdString) {
        this.clusterIdString = clusterIdString;
    }

    public NodeRec getControllerNode() {
        return controllerNodeRec;
    }
    public void setControllerNode(NodeRec controllerNodeRec) {
        this.controllerNodeRec = controllerNodeRec;
    }

    public List<NodeRec> getNodes() {
        return nodeRecList;
    }
    public void setNodes(List<NodeRec> nodeRecList) {
        this.nodeRecList = nodeRecList;
    }

    public DescribeClusterResultRec(
            Set<AclOperation> operationSet,
            String clusterIdString,
            Node controllerNode,
            Collection<Node> nodeCollection) {
        if (operationSet == null) {
            this.aclOperationList = null;
        } else {
            this.aclOperationList = new ArrayList<>(operationSet);
        }
        this.clusterIdString = clusterIdString;
        this.controllerNodeRec = new NodeRec(controllerNode);
        this.nodeRecList = nodeCollection
                .stream()
                .map(node -> new NodeRec(node))
                .collect(Collectors.toList());
    }
}
