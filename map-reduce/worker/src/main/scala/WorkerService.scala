import grpc._

import akka.stream.Materializer
import scala.concurrent.Future
import akka.NotUsed

import java.nio.file._

import com.google.protobuf.empty.Empty

class WorkerServiceImpl(app: MapReduceApp)(implicit mat: Materializer) extends Worker {
  import mat.executionContext

  // for intermediate
  private val base = Paths.get("tasks")

  // fake hdfs
  private val hdfsStub = Paths.get("hdfs")

  // private def emitIntermediate(key: String, )

  def map(request: MapRequest): Future[MapResponse] = {
    Future {
      request.inputFiles.flatMap{ file =>
        val content = Files.readString(hdfsStub.resolve(file.filename))
        
        val value = app.readerInput.read(content)
        app.map(file.filename, value)((k, v) => {
          // emit k - v
        })

        ???
      }

      // todo: partial result
      MapResponse(Nil)
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