package bluemold.concurrent

import org.bluemold.unsafe.Unsafe
import java.lang.IndexOutOfBoundsException

/**
 * AtomicArray<br/>
 * Author: Neil Essy<br/>
 * Created: 5/20/11<br/>
 * <p/>
 * [Description]
 */

object AtomicReferenceArray {
  val arrayOffset = Unsafe.arrayBaseOffset( classOf[Array[AnyRef]] )
  val arrayScale = Unsafe.arrayIndexScale( classOf[Array[AnyRef]] )
}
class AtomicReferenceArray[T <: AnyRef]( size: Int ) {
  import AtomicReferenceArray._

  val array = new Array[AnyRef]( size )
  def get( index: Int ): T = {
    if ( index < 0 || index >= size )
      throw new IndexOutOfBoundsException()
    Unsafe.getObjectVolatile( array, arrayOffset + index * arrayScale ).asInstanceOf[T]
  }
  def set( index: Int, value: T ) {
    if ( index < 0 || index >= size )
      throw new IndexOutOfBoundsException()
    Unsafe.putObjectVolatile( array, arrayOffset + index * arrayScale, value )
  }
  def compareAndSet( index: Int, expect: T, update: T ) {
    if ( index < 0 || index >= size )
      throw new IndexOutOfBoundsException()
    Unsafe.compareAndSwapObject( array, arrayOffset + index * arrayScale, expect, update )
  }
  def length = size
}