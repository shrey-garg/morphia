#!/usr/bin/env bash

copy() {
    DIR=`dirname $1`
    mkdir -p $DIR
    cp -v ~/dev/mongo-java-driver/bson/src/main/$1 $1
}

copy org/bson/codecs/configuration/package-info.java
copy org/bson/codecs/pojo/package-info.java
copy org/bson/codecs/package-info.java
