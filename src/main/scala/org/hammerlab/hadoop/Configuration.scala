package org.hammerlab.hadoop

import java.io.{ ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf
import org.apache.hadoop.conf.{ Configuration ⇒ HadoopConfiguration }
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.hammerlab.hadoop.kryo.WritableSerializer
import org.hammerlab.kryo._

/**
 * [[Serializable]] wrapper for a Hadoop [[conf.Configuration]]
 */
class Configuration(@transient var value: HadoopConfiguration)
  extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new HadoopConfiguration(false)
    value.readFields(in)
  }
}

object Configuration
  extends Registrar {

  def apply(loadDefaults: Boolean = true): Configuration =
    new HadoopConfiguration(loadDefaults)

  def apply(conf: HadoopConfiguration): Configuration =
    new Configuration(conf)

  implicit def wrapConfiguration(conf: HadoopConfiguration): Configuration =
    apply(conf)

  implicit def unwrapConfiguration(conf: Configuration): HadoopConfiguration =
    conf.value

  implicit def unwrapConfigurationBroadcast(confBroadcast: Broadcast[Configuration]): Configuration =
    confBroadcast.value

  implicit def sparkContextToHadoopConfiguration(sc: SparkContext): Configuration =
    sc.hadoopConfiguration

  implicit class Ops(val conf: HadoopConfiguration) extends AnyVal {
    def serializable: Configuration = conf
  }

  register(
    cls[conf.Configuration] → new WritableSerializer[conf.Configuration],
    classOf[Configuration] → serializeAs[Configuration, conf.Configuration]
  )
}
