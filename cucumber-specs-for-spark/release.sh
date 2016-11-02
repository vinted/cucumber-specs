#!/bin/bash

set -e

VERSION=$(cat VERSION)
SCALA_VERSIONS=${SCALA_VERSIONS:-2.10.6} # space-separated list
SPARK_VERSIONS=${SPARK_VERSIONS:-1.6.2} # space-separated list

for scala_version_full in $SCALA_VERSIONS; do
    for spark_version_full in $SPARK_VERSIONS; do
        spark_version=`ruby -e 'puts ARGV[0].split(".").slice(0, 2).join(".")' $spark_version_full`
        scala_version=`ruby -e 'puts ARGV[0].split(".").slice(0, 2).join(".")' $scala_version_full`
        full_version="${VERSION}_scala-${scala_version}_spark-${spark_version}"
        echo "Building cucumber-specs-for-spark-${full_version}"
        ./gradlew clean build publish \
                  -PscalaVersion=$scala_version_full \
                  -PsparkVersion=$spark_version_full \
                  -PlibraryVersion=$VERSION
    done
done
