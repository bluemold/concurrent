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
  val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicIntegerUnsafe], "value" )

  def create(): AtomicInteger = new AtomicIntegerUnsafe()
  def create( value: Int ): AtomicInteger = create().initialize( value )  
}

abstract class AtomicInteger {
  def get(): Int
  def set( value: Int )
  def compareAndSet( expect: Int, update: Int ): Boolean

  private[concurrent] final def initialize( value: Int ): AtomicInteger = { set( value ); this }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  @tailrec
  final def getAndSet(newValue: Int): Int =
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
  final def getAndIncrement(): Int =
  {
      val current: Int = get()
      val next: Int = current + 1
      if ( compareAndSet(current, next) ) current else getAndIncrement()
  }

  @tailrec
  final def getAndModIncrement( m: Int ): Int =
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
  final def getAndDecrement(): Int =
  {
      val current: Int = get()
      val next: Int = current - 1
      if ( compareAndSet(current, next) ) current else getAndDecrement()
  }

  @tailrec
  final def getAndModDecrement( m: Int ): Int =
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
  final def getAndAdd(delta: Int): Long =
  {
      val current: Int = get()
      val next: Int = current + delta
      if ( compareAndSet(current, next) ) current else getAndAdd( delta )
  }

  @tailrec
  final def getAndModAdd(delta: Int, m: Int ): Long =
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
  final def incrementAndGet(): Int =
  {
      val current: Int = get()
      val next: Int = current + 1
      if ( compareAndSet(current, next) ) next else incrementAndGet()
  }

  @tailrec
  final def modIncrementAndGet( m: Int ): Int =
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
  final def decrementAndGet(): Int =
  {
      val current: Int = get()
      val next: Int = current - 1
      if ( compareAndSet(current, next) ) next else decrementAndGet()
  }

  @tailrec
  final def modDecrementAndGet( m: Int ): Int =
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
  final def addAndGet(delta: Int): Int =
  {
      val current: Int = get()
      val next: Int = current + delta
      if ( compareAndSet(current, next) ) next else addAndGet( delta )
  }

  @tailrec
  final def modAddAndGet( delta: Int, m: Int ): Int =
  {
      val current: Int = get()
      val next: Int = ( current + delta ) % m
      if ( compareAndSet(current, next) ) next else modAddAndGet( delta, m )
  }

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override final def toString: String = get().toString
  final def booleanValue: Boolean = get().asInstanceOf[Boolean]
  final def charValue: Char = get().asInstanceOf[Char]
  final def shortValue: Short = get().asInstanceOf[Short]
  final def floatValue: Float = get().asInstanceOf[Float]
}

private final class AtomicIntegerUnsafe extends AtomicInteger {
  @volatile var value: Int = 0
  override def get(): Int = value
  override def set(value: Int) { this.value = value }
  override def compareAndSet(expect: Int, update: Int): Boolean = 
    Unsafe.compareAndSwapInt( this, AtomicInteger.unsafeValueIndex, expect, update )
}
