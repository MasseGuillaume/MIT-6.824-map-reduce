class EarlyExit0 extends MapReduceApp {

  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    emit(key, "1")
  }

  def reduce(key: String, values: Seq[String])(
      emit: String => Unit
  ): Unit = {
    if (key.contains("sherlock") || key.contains("tom")) {
      Thread.sleep(3000)
    }
    emit(values.size.toString())
  }
}
