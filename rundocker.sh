#!/bin/sh

CONTAINER_NAME=${1:-baldur}
docker run --rm --net host \
  --name $CONTAINER_NAME \
  -v /usr/share/spark/conf:/usr/local/spark/conf \
  -v /secure:/secure \
  -v /chi-ms-files-02:/chi-ms-files-02 \
  -v /mnt/talend/dev:/data \
  -v $PWD:/app \
  -it etspaceman/spark-docker 
