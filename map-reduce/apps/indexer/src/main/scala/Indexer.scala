class Indexer extends MapReduceApp {
  type InputValue = String

  type IntermediateKey = String
  type IntermediateValue = String

  
  type OutputValue = String

  val ordering: Ordering[String] = implicitly[Ordering[String]]

  val readerInput: Reader[String] = content => content
  val writerIntermediate: Writer[(String, String)] = { case (word, document) =>
    s"$word $document"
  }
  val writerKey: Writer[String] = key => key
  val readerIntermediate: Reader[(String, String)] = content => {
    val List(word, document) = content.split(' ').toList
    (word, document)
  }
  val writerOutput: Writer[String] = value => value

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
    emit(s"$key $count $documents")
  }
}
