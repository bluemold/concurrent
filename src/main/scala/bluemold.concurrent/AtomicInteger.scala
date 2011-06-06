package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

/**
 * AtomicInteger<br/>
 * Author: Neil Essy<br/>
 * Created: 4/22/11<br/>
 * <p/>
 * [Description]
 */

object AtomicInteger {
  private[concurrent] val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicInteger], "value" )

  def create(): AtomicInteger = new AtomicInteger()
  def create( value: Int ): AtomicInteger = new AtomicInteger( value )  
}

final class AtomicInteger( _default: Int ) {
  import AtomicInteger._

  def this() = this( 0 )

  @volatile private var value: Int = _default
  def get(): Int = value
  def set(value: Int) { this.value = value }
  def compareAndSet(expect: Int, update: Int): Boolean = 
    Unsafe.compareAndSwapInt( this, unsafeValueIndex, expect, update )

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  @tailrec
  def getAndSet(newValue: Int): Int =
  {
      val current: Int = get()
      if ( compareAndSet(current, newValue) ) current else getAndSet( newValue )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  @tailrec
  def getAndIncrement(): Int =
  {
      val current: Int = get()
      val next: Int = current + 1
      if ( compareAndSet(current, next) ) current else getAndIncrement()
  }

  @tailrec
  def getAndModIncrement( m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current + 1 ) % m
      if ( compareAndSet(current, next) ) current else getAndModIncrement( m )
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  @tailrec
  def getAndDecrement(): Int =
  {
      val current: Int = get()
      val next: Int = current - 1
      if ( compareAndSet(current, next) ) current else getAndDecrement()
  }

  @tailrec
  def getAndModDecrement( m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current - 1 ) % m
      if ( compareAndSet(current, next) ) current else getAndModDecrement( m )
  }
  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  @tailrec
  def getAndAdd(delta: Int): Long =
  {
      val current: Int = get()
      val next: Int = current + delta
      if ( compareAndSet(current, next) ) current else getAndAdd( delta )
  }

  @tailrec
  def getAndModAdd(delta: Int, m: Int ): Long =
  {
      val current: Int = get()
      val next: Int = ( current + delta ) % m
      if ( compareAndSet(current, next) ) current else getAndModAdd( delta, m )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  @tailrec
  def incrementAndGet(): Int =
  {
      val current: Int = get()
      val next: Int = current + 1
      if ( compareAndSet(current, next) ) next else incrementAndGet()
  }

  @tailrec
  def modIncrementAndGet( m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current + 1 ) % m
      if ( compareAndSet(current, next) ) next else modIncrementAndGet( m )
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  @tailrec
  def decrementAndGet(): Int =
  {
      val current: Int = get()
      val next: Int = current - 1
      if ( compareAndSet(current, next) ) next else decrementAndGet()
  }

  @tailrec
  def modDecrementAndGet( m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current - 1 ) % m
      if ( compareAndSet(current, next) ) next else modDecrementAndGet( m )
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  @tailrec
  def addAndGet(delta: Int): Int =
  {
      val current: Int = get()
      val next: Int = current + delta
      if ( compareAndSet(current, next) ) next else addAndGet( delta )
  }

  @tailrec
  def modAddAndGet( delta: Int, m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current + delta ) % m
      if ( compareAndSet(current, next) ) next else modAddAndGet( delta, m )
  }

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override def toString: String = get().toString
  def booleanValue: Boolean = get().asInstanceOf[Boolean]
  def charValue: Char = get().asInstanceOf[Char]
  def shortValue: Short = get().asInstanceOf[Short]
  def floatValue: Float = get().asInstanceOf[Float]
}
