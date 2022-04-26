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

## Install

Four easy steps...

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
to get the list of topics on your local cluster. Kafka clusters are defined in properties files in the "clusters" sub directory. The format of these properties files is the same as for the Kafka client API - you can re-use your properties files which you might have already created e.g. for your apps or for using the Kafka commandline tools.

