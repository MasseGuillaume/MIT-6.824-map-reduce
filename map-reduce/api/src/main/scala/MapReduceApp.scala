trait MapReduceApp {
  def partition(key: String, reducerCount: Int): Int = Math.abs(key.hashCode) % reducerCount
  def map(key: String, value: String)(emit: (String, String) => Unit): Unit
  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit
}