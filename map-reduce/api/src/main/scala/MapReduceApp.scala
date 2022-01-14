
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
  
  type OutputKey
  type OutputValue


  val ordering: Ordering[IntermediateKey]
  
  val readerInput: Reader[InputValue]
  val writerIntermediate: Writer[(IntermediateKey, IntermediateValue)]
  val readerIntermediate: Reader[(IntermediateKey, IntermediateValue)]
  val writerOutput: Writer[(OutputKey, OutputValue)]


  def map(key: String, value: InputValue)(emit: (IntermediateKey, IntermediateValue) => Unit): Unit
  def partition(key: IntermediateKey, reducerCount: Int): Int = key.hashCode % reducerCount
  def reduce(key: IntermediateKey, values: Seq[IntermediateValue])(emit: (OutputKey, OutputValue) => Unit): Unit
}