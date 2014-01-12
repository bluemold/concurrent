package bluemold.concurrent

import scala.concurrent.duration.Duration

trait TimeoutScheduler {
  def withTimeout[T]( future: Future[T], duration: Duration )
}
