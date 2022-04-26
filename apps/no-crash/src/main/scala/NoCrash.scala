// Same output as Crash, but never crash
class NoCrash extends MapReduceApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    emit("a", key)
    emit("b", key.size.toString)
    emit("c", value.size.toString)
    emit("d", "xyzzy")
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    // sort values to ensure deterministic output.
    emit(values.sorted.mkString(" "))
  }
}
