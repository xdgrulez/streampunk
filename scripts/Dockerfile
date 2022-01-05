FROM ghcr.io/graalvm/graalvm-ce:latest

ADD build/libs/streampunk-0.0.1-SNAPSHOT.jar /home/streampunk.jar
ADD build/resources/main* /home
ADD clusters* /home/clusters
ADD streampunk.py /home
ADD scripts/sppy /bin

RUN /bin/bash -c "gu install python"

RUN /bin/bash -c "chmod u+x /bin/sppy"

CMD tail -f /dev/null

# docker run -d streampunk:0.0.1
# docker exec -t <PID> /bin/bash

# sppy
# from streampunk import *
