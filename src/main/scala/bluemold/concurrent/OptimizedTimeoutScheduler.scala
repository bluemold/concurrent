package bluemold.concurrent

import scala.concurrent.duration.Duration
import scala.annotation.tailrec

final class OptimizedTimeoutScheduler( granularity: Long, grains: Int ) {
  private val buckets = Array.fill(grains) { new CancelableQueue[Future[_]] }
  private val heap = new CancelableHeapQueue[HeapTimeout]
  private class HeapTimeout( val future: Future[_], val time: Long ) extends Comparable[HeapTimeout] {
    def compareTo(o: HeapTimeout): Int = time.compareTo( o.time )
  }
  private val doShutdown = new AtomicBoolean() 
  private val timeoutThread: Thread = new Thread( new Runnable {
    def run() {
      while ( ! doShutdown.get() )
      {
        val now = System.currentTimeMillis()
        val granularTime = now / granularity
        val bucket = ( granularTime % grains ).toInt
        val queue = buckets(bucket)

        @tailrec
        def failBucket( future: Option[Future[_]] ) {
          future match {
            case None => // we are done
            case Some(f) =>
              f.fail()
              failBucket( queue.pop() )
          }
        }
        failBucket(queue.pop())


        val heapTimeoutNow = new HeapTimeout(null,now)
        @tailrec
        def failHeap( heapTimeout: Option[HeapTimeout] ) {
          heapTimeout match {
            case None => // we are done
            case Some(t) =>
              t.future.fail()
              failHeap( heap.popIfLessThanOrEqualTo(heapTimeoutNow) )
          }
        }
        failHeap( heap.popIfLessThanOrEqualTo(heapTimeoutNow) )

        val nextBucketDelay = granularity - now % granularity + granularity / 4
        val heapEntry = heap.peek()
        val nextDelay = heapEntry match {
          case None => nextBucketDelay
          case Some(timeout) =>
            if ( timeout.time <= now ) nextBucketDelay
            else Math.max( nextBucketDelay, timeout.time - now )
        }

        Thread.sleep( nextDelay )
      }
    }
  })
  timeoutThread.start()

  def withTimeout[T]( future: Future[T], duration: Duration ) {
    val now = System.currentTimeMillis()
    val millis = duration.toMillis
    val when = now + millis
    val delta = millis / granularity
    if ( delta <= 2 ) {
      val bucket = ( ( now / granularity + 2 ) % grains ).toInt
      val entry = buckets(bucket).push(future)
      future.onComplete( _ => entry.delete() )
    } else if ( delta < grains ) {
      val bucket = ( ( when / granularity ) % grains ).toInt
      val entry = buckets(bucket).push(future)
      future.onComplete( _ => entry.delete() )
    } else {
      val entry = heap.push(new HeapTimeout(future,when))
      future.onComplete( _ => entry.delete() )
    }
  }

  def shutdown() {
    doShutdown.set(value = true)
  }
}
