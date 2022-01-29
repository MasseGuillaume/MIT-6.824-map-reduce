class Indexer extends MapReduceApp {
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    for (word <- value.split("\\W+")) {
      emit(word, key)
    }
  }

  def reduce(key: String, values: Seq[String])(
      emit: String => Unit
  ): Unit = {
    val count = values.size
    val documents = values.sorted.mkString(",")
    emit(s"$count $documents")
  }
}