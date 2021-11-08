import sys
import java

# Admin
import org.xdgrulez.streampunk.admin.Acl as Acl
import org.xdgrulez.streampunk.admin.Cluster as Cluster
import org.xdgrulez.streampunk.admin.Group as Group
import org.xdgrulez.streampunk.admin.Topic as Topic

# Addon
import org.xdgrulez.streampunk.addon.Download as Download
import org.xdgrulez.streampunk.addon.Lookup as Lookup
import org.xdgrulez.streampunk.addon.Replicate as Replicate
import org.xdgrulez.streampunk.addon.SchemaRegistry as SchemaRegistry

def py(object):
    if isinstance(object, java.type("java.util.HashMap")):
        key_str_list = object.keySet().toArray()
        return { key_str: py(object.get(key_str)) for key_str in key_str_list }
    elif isinstance(object, java.type("org.xdgrulez.streampunk.record.OffsetsRec")):
        earliest_dict = py(object.getEarliest())
        latest_dict = py(object.getEarliest())
        return { "earliest": earliest_dict, "latest": latest_dict }
    else:
        return object
