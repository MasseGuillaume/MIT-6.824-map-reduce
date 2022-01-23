class WordCount extends MapReduceApp {
  type InputValue = String

  type IntermediateKey = String
  type IntermediateValue = Int

  type OutputKey = String
  type OutputValue = Int

  val ordering: Ordering[String] = implicitly[Ordering[String]]

  val readerInput: Reader[String] = content => content
  val writerIntermediate: Writer[(String, Int)] = { case (word, count) =>
    s"$word $count"
  }
  val writerKey: Writer[String] = key => key
  val readerIntermediate: Reader[(String, Int)] = content => {
    val List(word, count) = content.split(' ').toList
    (word, count.toInt)
  }
  val writerOutput: Writer[(String, Int)] = { case (word, count) =>
    s"$word $count"
  }

  def map(key: String, value: String)(emit: (String, Int) => Unit): Unit = {
    for (word <- value.split("\\W+")) {
      emit(word, 1)
    }
  }

  def reduce(key: String, values: Seq[Int])(
      emit: (String, Int) => Unit
  ): Unit = {
    emit(key, values.size)
  }
}
