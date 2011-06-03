package 

import org.bluemold.unsafe.Unsafe
import collection.immutable.HashMap
import annotation.tailrec
import collection.mutable.MapLike
import collection.mutable.Map
import collection.Iterator

/**
 * NonLockingHashMap<br/>
 * Author: Neil Essy<br/>
 * Created: 5/13/11<br/>
 * <p/>
 * [Description]
 */

private[concurrent] object NonLockingHashMap {

  val tableOffset = Unsafe.arrayBaseOffset( classOf[Array[HashMap[Any,Any]]] )
  val tableScale = Unsafe.arrayIndexScale( classOf[Array[HashMap[Any,Any]]] )

  val minimumConcurrency = 16
  val maximumConcurrency = 1 << 30

  def computeConcurrency( requestedConcurrency: Int ): Int = {
    @tailrec
    def helper( candidate: Int ): Int = {
      if ( candidate >= maximumConcurrency ) maximumConcurrency
      else if ( candidate >= requestedConcurrency ) candidate
      else helper( candidate * 2 )
    }
    if ( requestedConcurrency <= minimumConcurrency ) minimumConcurrency
    else helper( minimumConcurrency )
  }
}

final class NonLockingHashMap[A,B]( requestedConcurrency: Int ) extends Map[A, B] with MapLike[A, B, NonLockingHashMap[A, B]] {

  // default constructors
  def this() = this( NonLockingHashMap.minimumConcurrency )

  // initialization
  val concurrency = NonLockingHashMap.computeConcurrency( requestedConcurrency )
  val maps = Array.fill( concurrency )( new HashMap[A,B] )


  override def empty: this.type = {
    0 until concurrency foreach { index => empty0( index ) }
    this
  }

  @tailrec
  private def empty0( index: Int ) {
    val map = getHashMap( index )
    if ( ! updateHashMap( index, map, map.empty ) )
      empty0( index )
  }

  override def +=(kv: (A, B)): this.type = {
    val index = getHashMapIndex( kv._1 )
    @tailrec
    def helper( index: Int, kv: (A, B) ) {
      val hashMap = getHashMap( index )
      if ( ! updateHashMap( index, hashMap, hashMap + kv ) )
        helper( index, kv )
    }
    helper( index, kv )
    this
  }

  override def -=(key: A): this.type = {
    val index = getHashMapIndex( key )
    @tailrec
    def helper( index: Int, key: A ) {
      val hashMap = getHashMap( index )
      if ( ! updateHashMap( index, hashMap, hashMap - key ) )
        helper( index, key )
    }
    helper( index, key )
    this
  }

  override def iterator: Iterator[(A, B)] = {
    new ThisIterator( Array.tabulate( concurrency )( index => getHashMap( index ) ) )
  }

  private final class ThisIterator( maps: Array[HashMap[A,B]] ) extends Iterator[(A,B)] {
    var iterator: Iterator[(A,B)] = maps(0).iterator
    var mapIndex = 0

    @tailrec
    override def next(): (A, B) = {
      val hasNext = iterator.hasNext
      if ( hasNext ) iterator.next()
      else if ( mapIndex < concurrency - 1 ) {
        mapIndex += 1
        iterator = maps( mapIndex ).iterator
        next()
      } else throw new IllegalStateException( "No more elements" ) 
    }

    @tailrec
    override def hasNext: Boolean = {
      val _hasNext = iterator.hasNext
      if ( _hasNext ) true
      else if ( mapIndex < concurrency - 1 ) {
        mapIndex += 1
        iterator = maps( mapIndex ).iterator
        hasNext
      } else false 
    }
  }

  override def get(key: A): Option[B] = {
    getHashMap( getHashMapIndex( key ) ).get( key )
  }
  
  private def getHashMapIndex( key: A ): Int = {
    indexFor( hashFun( key.hashCode ) )
  }

  private def getHashMap( index: Int ): HashMap[A,B] = {
    Unsafe.getObjectVolatile( maps, NonLockingHashMap.tableOffset +
             index * NonLockingHashMap.tableScale ).asInstanceOf[HashMap[A,B]]
  } 

  private def updateHashMap( index: Int, expect: HashMap[A,B], update: HashMap[A,B] ): Boolean = {
    Unsafe.compareAndSwapObject( maps, NonLockingHashMap.tableOffset +
            index * NonLockingHashMap.tableScale, expect, update )
  } 

  private def hashFun( _h: Int ) = {
    val h = _h ^ ( ( _h >>> 20 ) ^ ( _h >>> 12 ) )
    h ^ ( h >>> 7 ) ^ ( h >>> 4 )
  }

  private def indexFor( h: Int ) = h & ( concurrency - 1 )
}