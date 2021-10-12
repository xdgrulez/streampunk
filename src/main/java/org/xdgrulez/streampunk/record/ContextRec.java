package org.xdgrulez.streampunk.record;

import javax.swing.plaf.nimbus.NimbusLookAndFeel;

public class ContextRec {
    private String clusterString;
    private String topicString;
    private String groupString;

    public String getCluster() {
        if (this.clusterString == null) {
            throw new NullPointerException("ContextRec.getCluster()");
        }
        return this.clusterString;
    }
    public void setCluster(String clusterString) {
        this.clusterString = clusterString;
    }

    public String getTopic() {
        if (this.topicString == null) {
            throw new NullPointerException("ContextRec.getTopic()");
        }
        return this.topicString;
    }
    public void setTopic(String topicString) {
        this.topicString = topicString;
    }

    public String getGroup() {
        if (this.groupString == null) {
            throw new NullPointerException("ContextRec.getGroup()");
        }
        return this.groupString;
    }
    public void setGroup(String groupString) {
        this.groupString = groupString;
    }

    public ContextRec(String clusterString, String topicString, String groupString) {
        this.clusterString = clusterString;
        this.topicString = topicString;
        this.groupString = groupString;
    }
}

