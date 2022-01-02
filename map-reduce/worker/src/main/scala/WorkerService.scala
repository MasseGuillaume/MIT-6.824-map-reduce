import grpc._

import akka.stream.Materializer
import scala.concurrent.Future
import akka.NotUsed

import java.nio.file._

import com.google.protobuf.empty.Empty

class WorkerServiceImpl(implicit mat: Materializer) extends Worker {
  import mat.executionContext

  // todo configurable
  private val job = new WordCount

  // for intermediate
  private val base = Paths.get("tasks")

  // fake hdfs
  private val hdfsStub = Paths.get("hdfs")

  // private def emitIntermediate(key: String, )

  def map(request: MapRequest): Future[MapResponse] = {
    Future {
      request.inputFiles.flatMap{ file =>
        val content = Files.readString(hdfsStub.resolve(file.filename))
        
        val value = job.readerV1.read(content)
        job.map(file.filename, value)((k, v) => {
          // emit k - v
        })

        ???
      }

      MapResponse(
        
      )
    }
  }

  def mapDone(in: Empty): Future[grpc.MapResponse] = {
    ??? 
  }

  def readIntermediateFile(in: IntermediateFile): Future[IntermediateFileContent] = {
    Future {
      IntermediateFileContent(
        Files.readString(base.resolve(in.filename))
      )
    }
  }

  def reduce(in: ReduceRequest): Future[ReduceResponse] = {
    ???
  }
}





// class WordCount extends MapReduceJob[String, String, Int, String, Int] {
//   val ordering: Ordering[String] = implicitly[Ordering[String]]

//   val readerV1: Reader[String] = content => content
//   val writerK2V2: Writer[(String, Int)] = { case (word, count) => s"$word $count" }
//   val writerK3V3: Writer[(String, Int)] = { case (word, count) => s"$word $count" }
//   val readerK2V2: Reader[(String, Int)] = content => {
//     val List(word, count) = content.split(' ').toList
//     (word, count.toInt)
//   }


//   def map(key: String, value: String)(emit: (String, Int) => Unit): Unit = {
//     for (word <- value.split("\\W+")) {
//       emit(word, 1)
//     }
//   }

//   def reduce(key: String, values: List[Int])(emit: (String, Int) => Unit): Unit = {
//     emit(key, values.size)
//   }
// }
