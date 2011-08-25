package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

/**
 * CancelableQueue<br/>
 * Author: Neil Essy<br/>
 * Created: 4/18/11<br/>
 * <p/>
 * [Description]
 */


private[concurrent] object CancelableQueue {
  import Unsafe._
  
  val deletedOffset = objectDeclaredFieldOffset( classOf[Entry[Any]], "deleted" )
  val nextOffset = objectDeclaredFieldOffset( classOf[Entry[Any]], "next" )
  val prevOffset = objectDeclaredFieldOffset( classOf[Entry[Any]], "prev" )

  abstract class Entry[T] {
    @volatile private[concurrent] var deleted: Int = 0
    @volatile private[concurrent] var prev: Entry[T] = null
    @volatile private[concurrent] var next: Entry[T] = null
    final def isInList = prev != null && next != null && deleted == 0
    def isHead = false
    def isTail = false
    private[concurrent] final def updateDeleted( expect: Int, update: Int ) =
      Unsafe.compareAndSwapInt( this, CancelableQueue.deletedOffset, expect, update )
    private[concurrent] final def updatePrev( expect: Entry[T], update: Entry[T] ) =
      Unsafe.compareAndSwapObject( this, CancelableQueue.prevOffset, expect, update )
    private[concurrent] final def updateNext( expect: Entry[T], update: Entry[T] ) =
      Unsafe.compareAndSwapObject( this, CancelableQueue.nextOffset, expect, update )
  }
  private[concurrent] class Head[T] extends Entry[T] {
    override def isHead = true
  }
  private[concurrent] class Tail[T] extends Entry[T] {
    override def isTail = true
  }
  private[concurrent] class Node[T]( _value: T ) extends Entry[T] {
    val value = _value
  }
}

final class CancelableQueue[T] {
  import CancelableQueue._
  val head = new Head[T]
  val tail = new Tail[T]
  head.updateNext( null, tail )
  tail.updatePrev( null, head )

  def getHead = head
  def getTail = tail
  
  def push( value: T ): Entry[T] = {
    insertBefore( tail, value )
  }

  def peek(): Option[T] = {
    head.next match {
      case candidate: Tail[T] => None
      case candidate: Node[T] => Some( candidate.value )
    }
  }

  @tailrec
  def pop(): Option[T] = {
    head.next match {
      case candidate: Tail[T] => None
      case candidate: Node[T] => {
        if ( delete( candidate ) ) Some( candidate.value )
        else pop()
      } 
    }
  }

  def insertBefore( entry: Entry[T], value: T ): Entry[T] = {
    if ( ! entry.isHead ) {
      insert( entry.prev, value )
    } else null
  }

  def insert( entry: Entry[T], value: T ): Entry[T] = {
    if ( ! entry.isTail && ( entry.isHead || entry.isInList ) ) {
      val newEntry = new Node( value )
      insert0( entry, newEntry )
      newEntry
    } else null
  }

  def delete( entry: Entry[T] ): Boolean = {
    if ( ! entry.isHead && ! entry.isTail ) {
      delete0( entry )
    } else false
  }
  
  @tailrec
  private def insert0( entry: Entry[T], newEntry: Entry[T] ) {
    val nextEntry = entry.next
    newEntry.updateNext( newEntry.next, nextEntry )
    newEntry.updatePrev( newEntry.prev, entry )
    if ( entry.updateNext( nextEntry, newEntry ) ) {
      // success now cleanup connections
      fixPrev( nextEntry, nextEntry.prev )
      cleanupNext( newEntry )
    } else insert0( entry, newEntry )
  }

  private def delete0( entry: Entry[T] ): Boolean = {
    if ( entry.updateDeleted( 0, 1 ) ) {
      // success, now do cleanup
      cleanupDeleted( entry ); true
    } else {
      // failed, do cleanup anyway
      cleanupDeleted( entry ); false
    }
  }
  
  private def cleanupDeleted( entry: Entry[T] ) {
    if ( entry.deleted != 0 ) {
      val next = entry.next
      val prev = entry.prev
      if ( next != null && prev != null ) {
        prev.updateNext( entry, next ) 
        next.updatePrev( entry, prev )
      }
    }
  }

  @tailrec
  private def fixPrev( entry: Entry[T], candidate: Entry[T] ) {
    if ( entry.deleted != 0 )
      cleanupDeleted( entry )
    else {
      if ( candidate.deleted != 0 ) {
        val candidatePrev = candidate.prev
        cleanupDeleted( candidate )
        fixPrev( entry, candidatePrev )
      } else {
        val currentPrev = entry.prev
        val candidateNext = candidate.next
        if ( candidateNext == entry ) {
          if ( currentPrev != candidate && ! entry.updatePrev( currentPrev, candidate ) )
            fixPrev( entry, entry.prev )
        } else fixPrev( entry, candidateNext )
      }
    } 
  }

  @tailrec
  private def cleanupNext( entry: Entry[T] ) {
    if ( entry.deleted != 0 )
      cleanupDeleted( entry )
    else {
      val next = entry.next
      if ( next.deleted != 0 ) {
        cleanupDeleted( next )
        cleanupNext( entry )
      }
    }
  }
}