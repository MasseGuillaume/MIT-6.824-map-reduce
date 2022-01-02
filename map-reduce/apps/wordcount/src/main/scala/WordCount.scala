class WordCount extends MapReduceJob[String, String, Int, String, Int] {
  val ordering: Ordering[String] = implicitly[Ordering[String]]

  val readerInput: Reader[String] = content => content
  val writerIntermediate: Writer[(String, Int)] = { case (word, count) => s"$word $count" }
  val readerIntermediate: Reader[(String, Int)] = content => {
    val List(word, count) = content.split(' ').toList
    (word, count.toInt)
  }
  val writerOutput: Writer[(String, Int)] = { case (word, count) => s"$word $count" }


  def map(key: String, value: String)(emit: (String, Int) => Unit): Unit = {
    for (word <- value.split("\\W+")) {
      emit(word, 1)
    }
  }

  def reduce(key: String, values: List[Int])(emit: (String, Int) => Unit): Unit = {
    emit(key, values.size)
  }
}
