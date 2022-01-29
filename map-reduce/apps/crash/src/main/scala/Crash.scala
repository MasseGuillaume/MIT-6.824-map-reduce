import scala.util.Random

class Crash extends MapReduceApp {

  private def maybeCrash(): Unit = {
    def random(max: Int): Int = Math.abs(Random.nextInt() % max)
    def max: Int = random(1000)
    if (max < 330) {
      // crash!
      throw new Exception("Boom")
    } else if (max < 660) {
      val slow = random(10000)
      println(s"slowdown: $slow ms")
      // Thread.sleep(slow)
    }
  }

  def map(key: String, value: String)(emit: (String, String) => Unit): Unit = {
    maybeCrash()

    emit("a", key)
    emit("b", key.size.toString)
    emit("c", value.size.toString)
    emit("d", "xyzzy")
  }

  def reduce(key: String, values: Seq[String])(emit: String => Unit): Unit = {
    maybeCrash()

    // sort values to ensure deterministic output.
    emit(values.sorted.mkString(" "))
  }
}
