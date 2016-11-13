# spark-util

[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-util_2.11.svg?maxAge=25920)](http://search.maven.org/#search%7Cga%7C1%7Cspark-util)

Low-level helpers for Spark libraries and tests, currently just a `KeyPartitioner` which assigns keys to partitions based on their value (if an `Int`) or the first value of a 2- or 3-tuple.
