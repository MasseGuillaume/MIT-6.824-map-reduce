import java.nio.file._
import java.io.PrintWriter

object Main {
  def main(args: Array[String]): Unit = {
    val app = FindMapReduceApp(Paths.get(args.head))
    val intermediate = Array.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

    for (filename <- args.tail) {
      val content = Files.readString(Paths.get(filename))
      val value = app.readerInput.read(content)
      app.map(filename, value){ (k, v) => 
        intermediate += k -> v
      }
    }
    val regions = intermediate.result().sortBy(_._1)(app.ordering)
    val writer = new PrintWriter("mr-out-0", "UTF-8")


    var i = 0
    while (i < regions.length) {
      var j = i + 1
      while (j < regions.length && regions(j)._1 == regions(i)._1) {
        j += 1
      }
      app.reduce(regions(i)._1, regions.slice(i, j).toList.map(_._2))(v =>
        writer.println(app.writerOutput.write(v))
      )
      i = j
    }

    writer.close()
  }
}