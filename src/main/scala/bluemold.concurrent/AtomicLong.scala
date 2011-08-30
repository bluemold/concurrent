package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

object AtomicLong {
  import Unsafe._
  private[concurrent] val noSpinValueIndex = objectDeclaredFieldOffset( classOf[AtomicLongNoSpin], "value" )
  private[concurrent] val unsafeValueIndex = objectDeclaredFieldOffset( classOf[AtomicLong], "value" )

  private[concurrent] val useUnsafe = isCs8supported

  def create(): AbstractAtomicLong = if ( useUnsafe ) new AtomicLong() else new AtomicLongNoSpin()
  def create( value: Long ): AbstractAtomicLong = if ( useUnsafe ) new AtomicLong( value ) else new AtomicLongNoSpin( value )  
}

abstract class AbstractAtomicLong {
  def get(): Long
  def set( value: Long )
  def compareAndSet( expect: Long, update: Long ): Boolean

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

final class AtomicLong( _default: Long ) extends AbstractAtomicLong {
  import AtomicLong._
  def this() = this( 0 )
  @volatile private var value: Long = _default
  override def get(): Long = value
  override def set(value: Long) { this.value = value }
  override def compareAndSet(expect: Long, update: Long): Boolean = 
    Unsafe.compareAndSwapLong( this, unsafeValueIndex, expect, update )
}

final class AtomicLongNoSpin( _default: Long ) extends AbstractAtomicLong {
  import AtomicLong._
  def this() = this( 0 )
  @volatile private var value: LongValue = LongValue( _default )
  override def get(): Long = value.value
  override def set(value: Long) { this.value = LongValue( value ) }
  override def compareAndSet(expect: Long, update: Long): Boolean = compareAndSet0( expect, LongValue( update ) ) 

  @tailrec
  private def compareAndSet0(expect: Long, updateValue: LongValue ): Boolean = {
    val currentValue = this.value
    if ( currentValue.value == expect ) {
      if ( Unsafe.compareAndSwapObject( this, noSpinValueIndex, currentValue, updateValue ) )
        true
      else
        compareAndSet0( expect, updateValue )
    } else false
  }
}