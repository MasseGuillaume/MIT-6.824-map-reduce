
class MapParallelApp extends ParallelApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    val ts = System.currentTimeMillis
    val pid = ProcessHandle.current().pid()
    val n = ParallelApp.countConcurrentJobs("map")
    println("MapParallelApp n=" + n)
    emit(s"times-$pid", ts.toString)
    emit(s"parallel-$pid", n.toString)
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit =
    emit(values.sorted.mkString(" "))
}