
class ReduceParallel extends ClassicMapReduceApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    ('a' to 'j').foreach(k =>
      emit(k.toString, "1")
    )
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    val n = Parallel.countConcurrentJobs("reduce")
    emit(key + " " + n.toString)
  }
}