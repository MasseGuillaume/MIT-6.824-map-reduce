import java.nio.file._
import java.io.PrintWriter

object Main {
  def main(args: Array[String]): Unit = {
    val List(appJarPath, className) = args.take(2).toList
    val app = FindMapReduceApp(Paths.get(appJarPath), className)
    val intermediate = Array.newBuilder[(String, String)]

    for (filename <- args.drop(2)) {
      val content = Files.readString(Paths.get(filename))
      app.map(filename, content){ (k, v) => 
        intermediate += k -> v
      }
    }
    val regions = intermediate.result().sortBy(_._1)
    val writer = new PrintWriter("mr-out-0", "UTF-8")


    var i = 0
    while (i < regions.length) {
      var j = i + 1
      while (j < regions.length && regions(j)._1 == regions(i)._1) {
        j += 1
      }
      app.reduce(regions(i)._1, regions.slice(i, j).toList.map(_._2))(v =>
        writer.println(s"${regions(i)._1} $v")
      )
      i = j
    }

    writer.close()
  }
}