package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.helper.Helpers;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

// Java Singletons Using Enum (https://dzone.com/articles/java-singletons-using-enum)
public enum AdminClientEnum {
    INSTANCE;

    private Map<String, AdminClient> stringAdminClientMap = new HashMap<>();

    public AdminClient get(String clusterString) {
        AdminClient adminClient = null;
        if (stringAdminClientMap.containsKey(clusterString)) {
            adminClient = stringAdminClientMap.get(clusterString);
        } else {
            var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
            adminClient = AdminClient.create(properties);
            this.stringAdminClientMap.put(clusterString, adminClient);
        }
        return adminClient;
    }
}
