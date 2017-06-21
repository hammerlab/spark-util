# spark-util

[![Build Status](https://travis-ci.org/hammerlab/spark-util.svg?branch=master)](https://travis-ci.org/hammerlab/spark-util)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-util/badge.svg)](https://coveralls.io/github/hammerlab/spark-util)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-util_2.11.svg?maxAge=1800)](http://search.maven.org/#search%7Cga%7C1%7Cspark-util)

Low-level helpers for Spark libraries and tests, currently:

- [`KeyPartitioner`](src/main/scala/org/hammerlab/spark/util/KeyPartitioner.scala): assigns keys to partitions based on their value (if an `Int`) or the first value of a 2- or 3-tuple
- type aliases for `Int`s: `PartitionIndex` and `NumPartitions`
- [`Conf`](src/main/scala/org/hammerlab/spark/Conf.scala): parses (comma-delimited) lists of properties files from a provided argument (as well as the `$SPARK_PROPERTIES_FILES` env var) and builds a `SparkConf` with those properties set.


