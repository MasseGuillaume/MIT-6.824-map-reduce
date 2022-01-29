class WordCount extends ClassicMapReduceApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    for (word <- value.split("\\W+")) {
      emit(word, "1")
    }
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    emit(s"$key ${values.size}")
  }
}
