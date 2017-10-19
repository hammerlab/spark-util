# spark-util

[![Build Status](https://travis-ci.org/hammerlab/spark-util.svg?branch=master)](https://travis-ci.org/hammerlab/spark-util)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/spark-util/badge.svg)](https://coveralls.io/github/hammerlab/spark-util)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/spark-util_2.11.svg?maxAge=1800)](http://search.maven.org/#search%7Cga%7C1%7Cspark-util)

Spark, Hadoop, and Kryo utilities

## Kryo registration

Classes that implement the [Registrar](src/main/scala/org/hammerlab/kryo/Registrar.scala) interface can use various shorthands for registering classes with Kryo.
 
 Adapted from [RegistrationTest](src/test/scala/org/hammerlab/kryo/RegistrationTest.scala):

```scala
register(
  cls[A],                  // comes with an AlsoRegister that loops in other classes
  arr[Foo],                // register a class and an Array of that class
  cls[B] → BSerializer(),  // use a custom Serializer
  CDRegistrar              // register all of another Registrar's registrations
)
```

- custom `Serializer`s and [`AlsoRegister`s](src/main/scala/org/hammerlab/kryo/AlsoRegister.scala) are picked up implicitly if not provided explicitly.
- `AlsoRegister`s are recursive, allowing for much easier and more robust accountability about what is registered and why, and ensurance that needed registrations aren't overlooked. 

## Configuration/Context wrappers
- [`Configuration`](src/main/scala/org/hammerlab/hadoop/Configuration.scala): serializable Hadoop-`Configuration` wrapper
- [`Context`](src/main/scala/org/hammerlab/spark/Context.scala): `SparkContext` wrapper that is also a Hadoop `Configuration`, for unification of "global configuration access" patterns
- [`Conf`](src/main/scala/org/hammerlab/spark/Conf.scala): load a `SparkConf` with settings from file(s) specified in the `SPARK_PROPERTIES_FILES` environment variable

## Spark Configuration
- [`SparkConfBase`](src/main/scala/org/hammerlab/spark/SparkConfBase.scala): trait that brokers setting config key-values and creating a `SparkConf`
- many mix-ins for common spark-configuration groups:
	- [kryo registration](src/main/scala/org/hammerlab/spark/confs/Kryo.scala)
	- [dynamic allocation](src/main/scala/org/hammerlab/spark/confs/DynamicAllocation.scala)
	- [event-logging](src/main/scala/org/hammerlab/spark/confs/EventLog.scala)
	- [task-speculation](src/main/scala/org/hammerlab/spark/confs/Speculation.scala)

## Misc

- [`KeyPartitioner`](src/main/scala/org/hammerlab/spark/KeyPartitioner.scala) / [`Partitioner`](src/main/scala/org/hammerlab/spark/Partitioner.scala): shorthands for common Spark-`Partitioner`-creation patterns
 	- from the first field of tuple-like objects
 	- from a partial function
 	- from a function
- [`Histogram` accumulator](src/main/scala/org/hammerlab/spark/accumulator/Histogram.scala)
