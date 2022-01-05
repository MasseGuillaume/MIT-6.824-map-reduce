import java.nio.file._



object Main {
  def main(args: Array[String]): Unit = {

    val app = FindMapReduceJob(Paths.get(args.head))
    
    val intermediate = List.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

    for (filename <- args.tail) {
      val content = Files.readString(Paths.get(filename))
      val value = app.readerInput.read(content)
      app.map(filename, value){ (k, v) => 
        intermediate += k -> v
      }
    }

    val output = List.newBuilder[String]

    val regions = intermediate.result().sortBy(_._1)(app.ordering)
    var i = 0
    while (i < regions.length) {
      var j = i + 1
      while (j < regions.length && regions(j)._1 == regions(i)._1) {
        j += 1
      }
      val values = List.newBuilder[app.IntermediateValue]

      for (k <- i until j) {
        values += regions(k)._2
      }
      app.reduce(regions(i)._1, values.result()){ (k, v) =>
        output += app.writerOutput.write(k -> v)
      }
      
      i = j
    }

    Files.writeString(Paths.get("mr-out-0"), output.result().mkString("\n"))
  }
}