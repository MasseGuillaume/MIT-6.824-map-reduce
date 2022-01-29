trait Reader[V] {
  def read(input: String): V
}

trait Writer[V] {
  def write(output: V): String
}

trait MapReduceApp {

  type InputValue

  type IntermediateKey
  type IntermediateValue

  type OutputValue

  val ordering: Ordering[IntermediateKey]

  val readerInput: Reader[InputValue]
  val writerIntermediate: Writer[(IntermediateKey, IntermediateValue)]
  val readerIntermediate: Reader[(IntermediateKey, IntermediateValue)]
  val writerKey: Writer[IntermediateKey]
  val writerOutput: Writer[OutputValue]

  def map(key: String, value: InputValue)(
      emit: (IntermediateKey, IntermediateValue) => Unit
  ): Unit
  def partition(key: IntermediateKey, reducerCount: Int): Int =
    Math.abs(key.hashCode) % reducerCount

  def reduce(key: IntermediateKey, values: Seq[IntermediateValue])(
      emit: OutputValue => Unit
  ): Unit
}

trait ClassicMapReduceApp extends MapReduceApp {
  type InputValue = String

  type IntermediateKey = String
  type IntermediateValue = String

  
  type OutputValue = String

  val ordering: Ordering[String] = implicitly[Ordering[String]]

  val readerInput: Reader[String] = content => content
  val writerIntermediate: Writer[(String, String)] = { case (key, value) =>
    s"$key $value"
  }
  val writerKey: Writer[String] = key => key
  val readerIntermediate: Reader[(String, String)] = content => {
    val List(key, value) = content.split(' ').toList
    (key, value)
  }
  val writerOutput: Writer[String] = value => value
}

object WorkerUtils {
  def portFromIndex(index: Int): Int = 8000 + index
}