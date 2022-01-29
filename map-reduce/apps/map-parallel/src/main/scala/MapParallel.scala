
class MapParallel extends ClassicMapReduceApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    val ts = System.nanoTime
    val pid = ProcessHandle.current().pid()
    val n = Parallel.countConcurrentJobs("map")
    emit(s"times-$pid", ts.toString)
    emit(s"parallel-$pid", n.toString)
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit =
    emit(key + " " + values.sorted.mkString(" "))
}