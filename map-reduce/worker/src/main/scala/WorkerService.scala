import grpc._

import akka.stream.Materializer
import scala.concurrent.Future
import akka.NotUsed

import java.nio.file._

import com.google.protobuf.empty.Empty

class WorkerServiceImpl(app: MapReduceApp, reducerCount: Int)(implicit mat: Materializer) extends Worker {
  import mat.executionContext

  // config
  private val maxBufferSize = 32000
  private val partitions = 10

  // for intermediate
  private val base = Paths.get("tasks")

  // fake hdfs
  private val hdfsStub = Paths.get("hdfs")

  private var bufferSize = 0
  private var buffer = List.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

  private def storeIntermediate(): List[IntermediateFile] = {
    val intermediate =
      buffer.result().groupBy(x => app.partition(x._1, reducerCount)).toList.map {
        case (region, kvs) =>
          val id = java.util.UUID.randomUUID
          val content = kvs.map(app.writerIntermediate.write).mkString("\n")
          val path = base.resolve(s"$id-$region")
          Files.writeString(path, content)
          IntermediateFile(path.toString)
      }
    
    bufferSize = 0
    buffer = List.newBuilder[(app.IntermediateKey, app.IntermediateValue)]

    intermediate
  }

  def map(request: MapRequest): Future[MapResponse] = {
    Future {
      request.inputFiles.foreach{ file =>
        val content = Files.readString(hdfsStub.resolve(file.filename))
        val value = app.readerInput.read(content)
        app.map(file.filename, value)((k, v) => {
          bufferSize += 1
          buffer += k -> v
        })
      }
      if (bufferSize < maxBufferSize) {
        MapResponse(Nil)
      } else {
        MapResponse(storeIntermediate())
      }
    }
  }

  def mapDone(in: Empty): Future[MapResponse] = {
    Future(MapResponse(storeIntermediate()))
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