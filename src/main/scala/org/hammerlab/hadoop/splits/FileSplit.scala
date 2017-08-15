package org.hammerlab.hadoop.splits

import org.apache.hadoop.fs.{ Path ⇒ HPath }
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapred
import org.hammerlab.paths.Path

/**
 * Case-class sugar over Hadoop [[input.FileSplit]]
 */
case class FileSplit(path: Path,
                     start: Long,
                     length: Long,
                     locations: Array[String]) {
  def end = start + length
}

object FileSplit {
  implicit def apply(split: input.FileSplit): FileSplit =
    FileSplit(
      Path(split.getPath.toUri),
      split.getStart,
      split.getLength,
      split.getLocations
    )

  implicit def apply(split: mapred.FileSplit): FileSplit =
    FileSplit(
      Path(split.getPath.toUri),
      split.getStart,
      split.getLength,
      split.getLocations
    )

  implicit def apply(split: FileSplit): input.FileSplit =
    new input.FileSplit(
      new HPath(split.path.toUri),
      split.start,
      split.length,
      split.locations
    )
}
