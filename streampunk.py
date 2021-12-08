import sys
from typing import MappingView
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

# Consumer
import org.xdgrulez.streampunk.consumer.ConsumerAvro as ConsumerAvro
import org.xdgrulez.streampunk.consumer.ConsumerByteArray as ConsumerByteArray
import org.xdgrulez.streampunk.consumer.ConsumerString as ConsumerString
import org.xdgrulez.streampunk.consumer.ConsumerStringAvro as ConsumerStringAvro
import org.xdgrulez.streampunk.consumer.ConsumerStringProtobuf as ConsumerStringProtobuf

# Producer
import org.xdgrulez.streampunk.producer.ProducerAvro as ProducerAvro
import org.xdgrulez.streampunk.producer.ProducerByteArray as ProducerByteArray
import org.xdgrulez.streampunk.producer.ProducerString as ProducerString
import org.xdgrulez.streampunk.producer.ProducerStringAvro as ProducerStringAvro
import org.xdgrulez.streampunk.producer.ProducerStringProtobuf as ProducerStringProtobuf

# Helpers
import org.xdgrulez.streampunk.helper.Helpers as Helpers

def jp(object):
    def is_sp_rec_object(object):
        return hasattr(object, "getClass") and hasattr(object.getClass(), "getName") and object.getClass().getName().startswith("org.xdgrulez.streampunk.record.")

    def method_str_to_key_str(method_str):
        return method_str[3].lower() + method_str[4:]

    if isinstance(object, java.type("java.util.ArrayList")):
        return [jp(list_element_object) for list_element_object in object]
    elif isinstance(object, java.type("java.util.HashMap")):
        key_str_list = object.keySet().toArray()
        return {key_str: jp(object.get(key_str)) for key_str in key_str_list}
    elif is_sp_rec_object(object):
        getter_str_key_str_tuple_list = [(method_str, method_str_to_key_str(method_str)) for method_str in dir(object) if method_str.startswith("get")]
        return {key_str: jp(getattr(object, method_str)()) for (method_str, key_str) in getter_str_key_str_tuple_list}
    else:
        return object


def pj(object):
    if isinstance(object, dict):
        map = java.type("java.util.HashMap")()
        for key, value in object.items():
            map.put(key, pj(value))
        return map
    elif isinstance(object, int):
        long = java.type("java.lang.Long")(object)
        return long
    else:
        return object
