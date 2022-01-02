
trait Reader[V] {
  def read(input: String): V
}

trait Writer[V] {
  def write(output: V): String
}

trait MapReduceJob[V1, K2, V2, K3, V3] {
  type IntermediateKey = K2
  type IntermediateValue = V2
  type Intermediate = (K2, V2)

  type Output = (K3, V3)

  val ordering: Ordering[K2]
  
  val readerInput: Reader[V1]
  val writerIntermediate: Writer[(K2, V2)]
  val readerIntermediate: Reader[(K2, V2)]
  val writerOutput: Writer[(K3, V3)]


  def map(key: String, value: V1)(emit: (K2, V2) => Unit): Unit
  def partition(key: K2, reducerCount: Int): Int = key.hashCode % reducerCount
  def reduce(key: K2, values: List[V2])(emit: (K3, V3) => Unit): Unit
}