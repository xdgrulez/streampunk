# streampunk

```
       _____                                                    ______  
_________  /__________________ _______ _______________  ___________  /__
__  ___/  __/_  ___/  _ \  __ `/_  __ `__ \__  __ \  / / /_  __ \_  //_/
_(__  )/ /_ _  /   /  __/ /_/ /_  / / / / /_  /_/ / /_/ /_  / / /  ,<   
/____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/_  .___/\__,_/ /_/ /_//_/|_|  
                                           /_/
```

The Difference Engine for Unlocking the Kafka Black Box

Started by Ralph M. Debusmann, CTO of Forecasty.AI - building No-Code Deep Tech for the future.

Check out our new Commodity Desk where commodity price forecasting meets Natural Language Processing:
https://forecasty.ai/offerings/commodity-desk/

## How Can I Run It?

1. To run Streampunk, you first have to download and install GraalVM for Java 11 here: https://www.graalvm.org/downloads/. After you have installed GraalVM, make sure that you install the programming languages that you like most - e.g. "gu install python" gives you Python-support, "gu install R" is for R etc. (this is documented here: https://www.graalvm.org/22.0/docs/getting-started/).

2. Next, go to the Streampunk directory and use GraalVM (you could actually use any JVM, as long as it matches the Java version of GraalVM) to compile Streampunk and produce the corresponding JAR file:
```
./gradlew build
```

3. Now to use Streampunk through one of the languages supported by GraalVM, you need to run GraalVM-interpreter for that language. For Python, you call the graalpython interpreter as follows:
```
graalpython --polyglot --jvm --experimental-options --python.EmulateJython --vm.cp=build/libs/streampunk-0.0.1-SNAPSHOT.jar

```

4. For Python, I've already written a small wrapper which you should import to really make use of Streampunk:
```
from sp import *
```

5. You can start doing stuff, e.g. you can call
```
Topic.list("local")
```
to get the list of topics on your local cluster.

## Cluster Definitions

Kafka clusters are defined in properties files in the "clusters" sub directory. The format of these properties files is the same as for the Kafka client API - you can re-use your properties files which you might have already created e.g. for your apps or for using the Kafka commandline tools.

## Streampunk Commands

Not yet documented - please have a look at the following Java files in the "src/main/java/org/xdgrulez/streampunk/" directory:
* admin/Acl.java - ACL commands
* admin/Cluster.java - cluster commands
* admin/Group.java - consumer groups commands
* admin/Topic.java - topics commands
* consumer/... - consumer commands
* producer/... - producer commands
* addon/... - add-ons, e.g. for replications (with or without message transforms)

Please help me documenting all this stuff!

## Java-Python Interop

Some of the commands return Java objects, so you might end up with something like this:
```
>>> Topic.getConfig("local", "input")
<JavaObject[java.util.HashMap] at 0x616d9a6d>
```

To see what's inside the Java object, I've build them in a way that you can easily "decode" them. The Python wrapper sp.py includes two convenience functions for transforming Java objects to Python dictionaries, and vice versa. So you can decode the Java object in the example above by using the function "jp" (stands for "Java-to-Python") as follows:
```
>>> jp(Topic.getConfig("local", "input"))
{'compression.type': 'producer', 'redpanda.remote.read': 'false', 'redpanda.datapolicy': 'function_name:  script_name: ', 'cleanup.policy': 'delete', 'partition_count': '1', 'replication_factor': '1', 'message.timestamp.type': 'CreateTime', 'segment.bytes': '1073741824', 'retention.ms': '604800000', 'retention.bytes': '-1', 'redpanda.remote.write': 'false'}
```

You can use the other way around e.g. if you would like to specify the start offset for consuming a topic - in this case, you use the function "pj" ("Python-to-Java") to convert a Python dictionary to the corresponding Java object:
```
>>> ConsumerString.consume("local", "input", "test", pj({0:3}))

Topic: input, Partition: 0, Offset: 3, Timestamp: 2022-04-22 13:24:52.781
Headers:
Key: null
Value: z

Topic: input, Partition: 0, Offset: 4, Timestamp: 2022-04-22 13:25:08.769
Headers:
Key: null
Value: z

Topic: input, Partition: 0, Offset: 5, Timestamp: 2022-04-22 13:25:21.998
Headers:
Key: null
Value: z

Continue (Y/n)? n
None
```

If more people contribute to Streampunk, we can of course implement all these conversions directly inside the language wrapper (e.g. sp.py), I just haven't done it yet because Streampunk is still very volatile and basically whenever you would change anything in the Streampunk JAR, you would also have to change all the language wrappers accordingly, which I have yet tried to avoid...

## Support for Languages Other than Python

Not yet done - would be easy to add wrappers similar to sp.py for Javascript, R, Ruby... please help here as well!

