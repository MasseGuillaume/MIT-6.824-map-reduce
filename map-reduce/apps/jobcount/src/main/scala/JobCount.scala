// a MapReduce pseudo-application that counts the number of times map/reduce
// tasks are run, to test whether jobs are assigned multiple times even when
// there is no failure.

import scala.util.Random

import java.nio.file._
import java.io.File

object Count {
  var count = 0
}

class JobCount extends MapReduceApp {
  import Count.count

  private val prefix = "mr-worker-jobcount"

  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    val pid = ProcessHandle.current().pid()
    Files.writeString(Paths.get(s"$prefix-$pid-$count"), "x")
    count += 1
    Thread.sleep(2000 + Math.abs(Random.nextInt() % 3000))
    emit("a", "x")
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    val invocations =
      new File(".").listFiles().count(_.getName.startsWith(prefix))
    emit(invocations.toString)
  }
}
