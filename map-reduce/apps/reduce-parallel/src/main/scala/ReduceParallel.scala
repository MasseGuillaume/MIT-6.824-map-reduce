
class ReduceParallelApp extends ParallelApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    ('a' to 'j').foreach(k =>
      emit(k, "1")
    )
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    val n = ParallelApp.countConcurrentJobs("reduce")
    println("ReduceParallelApp n=" + n)
    emit(n)
  }
}