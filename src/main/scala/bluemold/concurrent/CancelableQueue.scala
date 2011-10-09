package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

object CancelableQueue {
  import Unsafe._
  
  private val deletedOffset = objectDeclaredFieldOffset( classOf[Entry[_]], "deleted" )
  private val nextOffset = objectDeclaredFieldOffset( classOf[Entry[_]], "next" )
  private val prevOffset = objectDeclaredFieldOffset( classOf[Entry[_]], "prev" )

  sealed abstract class Entry[T] {
    @volatile private[concurrent] var deleted: Int = 0
    @volatile private[concurrent] var prev: Entry[T] = null
    @volatile private[concurrent] var next: Entry[T] = null
    final def isInList = prev != null && next != null && deleted == 0
    def isHead = false
    def isTail = false
    def delete(): Boolean = throw new RuntimeException( "Can not delete head or tail" )
    private[concurrent] final def updateDeleted( expect: Int, update: Int ) =
      Unsafe.compareAndSwapInt( this, CancelableQueue.deletedOffset, expect, update )
    private[concurrent] final def updatePrev( expect: Entry[T], update: Entry[T] ) =
      Unsafe.compareAndSwapObject( this, CancelableQueue.prevOffset, expect, update )
    private[concurrent] final def updateNext( expect: Entry[T], update: Entry[T] ) =
      Unsafe.compareAndSwapObject( this, CancelableQueue.nextOffset, expect, update )
  }
}

final class CancelableQueue[T] {
  import CancelableQueue._

  final private[concurrent] class Head extends Entry[T] {
    override def isHead = true
  }
  final private[concurrent] class Tail extends Entry[T] {
    override def isTail = true
  }
  final private[concurrent] class Node( _value: T ) extends Entry[T] {
    val value = _value
    override def delete() = CancelableQueue.this.delete( this )
  }

  val head = new Head
  val tail = new Tail
  head.updateNext( null, tail )
  tail.updatePrev( null, head )

  def getHead = head
  def getTail = tail
  
  def push( value: T ): Entry[T] = {
    insertBefore( tail, value )
  }

  def peek(): Option[T] = {
    val next = head.next
    if ( next.isInstanceOf[Tail] ) None
    else if ( next.isInstanceOf[Node] ) Some( next.asInstanceOf[Node].value )
    else throw new RuntimeException( "What Happened!" )
/*
    head.next match {
      case candidate: Tail => None
      case candidate: Node => Some( candidate.value )
      case _ => throw new RuntimeException( "What Happened!" )
    }
*/
  }

  @tailrec
  def pop(): Option[T] = {
    val next = head.next
    if ( next.isInstanceOf[Tail] ) None
    else if ( next.isInstanceOf[Node] ) {
      if ( delete( next ) ) Some( next.asInstanceOf[Node].value )
      else pop()
    } else throw new RuntimeException( "What Happened!" )
/*
    head.next match {
      case candidate: Tail => None
      case candidate: Node => {
        if ( delete( candidate ) ) Some( candidate.value )
        else pop()
      } 
      case _ => throw new RuntimeException( "What Happened!" )
    }
*/
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