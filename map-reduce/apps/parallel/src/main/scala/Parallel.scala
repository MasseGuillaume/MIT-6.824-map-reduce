import java.nio.file._
import java.io.File

object Parallel {
  def countConcurrentJobs(phase: String): Int = {
    val prefix = s"mr-worker-$phase-"

    // create a file so that other workers will see that
    // we're running at the same time as them.
    val pid = ProcessHandle.current().pid()
    val pidFile = Paths.get(s"$prefix$pid")
    Files.write(pidFile, "x".getBytes())

    // are any other workers running?
    // find their PIDs by scanning directory for mr-worker-XXX files.
    val runningProcesses = 
      new File(".")
        .listFiles()
        .map(_.getName)
        .filter(_.startsWith(prefix))
        .count{ filename =>
          val otherPid = filename.stripPrefix(prefix)

          if (pid.toString != otherPid) {
            import scala.sys.process._
            // kill -0 just checks if the proccess is alive
            ("kill" :: "-0" :: otherPid :: Nil).! == 0
          } else {
            true // this process is running
          }
        }

    Thread.sleep(1000)
    if (Files.exists(pidFile)) {
      Files.delete(pidFile)
    } else {
      sys.error(s"pid $pid is running more than one task")
      sys.exit(1)
    }

    runningProcesses
  }
}