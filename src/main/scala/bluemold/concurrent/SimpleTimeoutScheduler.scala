package bluemold.concurrent

import scala.concurrent.duration.Duration
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class SimpleTimeoutScheduler( executor: ScheduledExecutorService )
    extends TimeoutScheduler {
   
  def withTimeout[T](future: Future[T], duration: Duration) {
    val timeout = executor.schedule( new Runnable { 
      def run() { future.fail() }
    }, duration.toMillis, TimeUnit.MILLISECONDS )
    future.onComplete( _ => timeout.cancel(false) )
  }
}
