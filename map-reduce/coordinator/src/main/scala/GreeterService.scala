import grpc._

import akka.stream.Materializer
import scala.concurrent.Future
import akka.NotUsed

class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  import mat.executionContext

  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    println(s"sayHello to ${in.name}")
    Future.successful(
      HelloReply(s"Hello, ${in.name}")
    )
  }
}
