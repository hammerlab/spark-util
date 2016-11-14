# spark-util

[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-util_2.11.svg?maxAge=1800)](http://search.maven.org/#search%7Cga%7C1%7Cspark-util)

Low-level helpers for Spark libraries and tests, currently:

- a `KeyPartitioner` which assigns keys to partitions based on their value (if an `Int`) or the first value of a 2- or 3-tuple.
- type aliases for `Int`s: `PartitionIndex` and `NumPartitions`.

