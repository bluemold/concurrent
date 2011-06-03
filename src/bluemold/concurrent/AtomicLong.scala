package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

/**
 * AtomicLong<br/>
 * Author: Neil Essy<br/>
 * Created: 4/22/11<br/>
 * <p/>
 * [Description]
 */

object AtomicLong {
  import Unsafe._
  val noSpinValueIndex = objectDeclaredFieldOffset( classOf[AtomicLongNoSpin], "value" )
  val unsafeValueIndex = objectDeclaredFieldOffset( classOf[AtomicLongUnsafe], "value" )

  val useUnsafe = isCs8supported

  def create(): AtomicLong = if ( useUnsafe ) new AtomicLongUnsafe() else new AtomicLongNoSpin()
  def create( value: Long ): AtomicLong = create().initialize( value )  
}

abstract class AtomicLong {
  def get(): Long
  def set( value: Long )
  def compareAndSet( expect: Long, update: Long ): Boolean

  private[concurrent] final def initialize( value: Long ): AtomicLong = { set( value ); this }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  @tailrec
  final def getAndSet(newValue: Long): Long =
  {
      val current: Long = get()
      if ( compareAndSet(current, newValue) ) current else getAndSet( newValue )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  @tailrec
  final def getAndIncrement(): Long =
  {
      val current: Long = get()
      val next: Long = current + 1
      if ( compareAndSet(current, next) ) current else getAndIncrement()
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  @tailrec
  final def getAndDecrement(): Long =
  {
      val current: Long = get()
      val next: Long = current - 1
      if ( compareAndSet(current, next) ) current else getAndDecrement()
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  @tailrec
  final def getAndAdd(delta: Long): Long =
  {
      val current: Long = get()
      val next: Long = current + delta
      if ( compareAndSet(current, next) ) current else getAndAdd( delta )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  @tailrec
  final def incrementAndGet(): Long =
  {
      val current: Long = get()
      val next: Long = current + 1
      if ( compareAndSet(current, next) ) next else incrementAndGet()
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  @tailrec
  final def decrementAndGet(): Long =
  {
      val current: Long = get()
      val next: Long = current - 1
      if ( compareAndSet(current, next) ) next else decrementAndGet()
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  @tailrec
  final def addAndGet(delta: Long): Long =
  {
      val current: Long = get()
      val next: Long = current + delta
      if ( compareAndSet(current, next) ) next else addAndGet( delta )
  }

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override final def toString: String = get().toString
  final def intValue: Int = get().asInstanceOf[Int]
  final def floatValue: Float = get().asInstanceOf[Float]
  final def doubleValue: Double = get().asInstanceOf[Double]
}

private final case class LongValue( value: Long )

private final class AtomicLongUnsafe extends AtomicLong {
  @volatile var value: Long = 0
  override def get(): Long = value
  override def set(value: Long) { this.value = value }
  override def compareAndSet(expect: Long, update: Long): Boolean = 
    Unsafe.compareAndSwapLong( this, AtomicLong.unsafeValueIndex, expect, update )
}

private final class AtomicLongNoSpin extends AtomicLong {
  @volatile var value: LongValue = LongValue( 0 )
  override def get(): Long = value.value
  override def set(value: Long) { this.value = LongValue( value ) }
  override def compareAndSet(expect: Long, update: Long): Boolean = compareAndSet0( expect, LongValue( update ) ) 

  @tailrec
  def compareAndSet0(expect: Long, updateValue: LongValue ): Boolean = {
    val currentValue = this.value
    if ( currentValue.value == expect ) {
      if ( Unsafe.compareAndSwapObject( this, AtomicLong.noSpinValueIndex, currentValue, updateValue ) )
        true
      else
        compareAndSet0( expect, updateValue )
    } else false
  }
}