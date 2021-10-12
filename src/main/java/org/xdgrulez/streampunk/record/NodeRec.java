package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.Node;

public class NodeRec {
    public int idInt;
    public String hostString;
    public int portInt;
    public String rackString;

    public int getId() {
        return idInt;
    }
    public void setId(int idInt) {
        this.idInt = idInt;
    }

    public String getHost() {
        return hostString;
    }
    public void setHost(String hostString) {
        this.hostString = hostString;
    }

    public int getPort() {
        return portInt;
    }
    public void setPort(int portInt) {
        this.portInt = portInt;
    }

    public String getRack() {
        return rackString;
    }
    public void setRack(String rackString) {
        this.rackString = rackString;
    }

    public NodeRec(int idInt, String hostString, int portInt, String rackString) {
        this.idInt = idInt;
        this.hostString = hostString;
        this.portInt = portInt;
        this.rackString = rackString;
    }

    public NodeRec(Node node) {
        this.idInt = node.id();
        this.hostString = node.host();
        this.portInt = node.port();
        this.rackString = node.rack();
    }

    public Node node() {
        return new Node(this.idInt, this.hostString, this.portInt, this.rackString);
    }
}
