package org.hammerlab.hadoop.splits

object PartFileBasename {
  val regex = """part-(\d+)""".r
  def unapply(str: String): Option[Int] =
    str match {
      case regex(digits) ⇒ Some(digits.toInt)
      case _ ⇒ None
    }

  val format = "part-%05d"
  def apply(idx: Int): String = format.format(idx)
}
