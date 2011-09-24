package bluemold.concurrent

import annotation.tailrec

/**
 * Provides an inequality comparison that mimics comparisons done by object pointer in C, C++. This
 * allows for the use of algorithms that need and intrinsic higher or lower object comparison
 * for asymmetry. All objects that implement this trait can be compared to one another.
 * This comparison is guaranteed to be 0 only when the comparison object is exactly the same object.
 * The inequality relationship is guaranteed and consistent. If A < B and B < C then A < C and these
 * inequalities will remain true throughout the lifetimes of the objects.
 * 
 * Because the intrinsic identity is potentially updated over time by any number of threads this
 * is designed to be non blocking and can never live/dead lock as it always makes forward progress.
 * 
 * The underlying mechanism is to maintain a hashCode sequence that is added to as needed. The
 * first hashCode is of the object itself. The second hashCode is of the AtomicReference that
 * holds the sequence list. By the time a third hashCode is needed a List object has been created to
 * hold the second hashCode and so from that point forward the next hashCode is always from the latest
 * List object created in appending the previous hashCode to the sequence.
 **/

trait IntrinsicallyComparable {
  final private val intrinsicIdentity = new AtomicReference[List[Int]]( Nil )

  final def intrinsicCompareTo( other: IntrinsicallyComparable ): Int = {
    if ( this eq other ) 0
    else {
      val hashCode = System.identityHashCode( this )
      val otherHashCode = System.identityHashCode( other )
      if ( hashCode == otherHashCode ) intrinsicCompareTo0( other )
      else if ( hashCode < otherHashCode ) -1 else 1
    }
  }

  @tailrec
  final private def intrinsicCompareTo0( other: IntrinsicallyComparable ): Int = {
    var identity = intrinsicIdentity.get()
    var otherIdentity = other.intrinsicIdentity.get()
    if ( identity == Nil ) {
      intrinsicIdentity.compareAndSet( identity, System.identityHashCode( intrinsicIdentity ) :: Nil )
      if ( otherIdentity == Nil )
        other.intrinsicIdentity.compareAndSet( otherIdentity, System.identityHashCode( other.intrinsicIdentity ) :: Nil )
      intrinsicCompareTo0( other )
    } else if ( otherIdentity == Nil ) {
      other.intrinsicIdentity.compareAndSet( otherIdentity, System.identityHashCode( other.intrinsicIdentity ) :: Nil )
      intrinsicCompareTo0( other )
    } else {
      var identA = if ( identity.tail == Nil ) identity else identity.reverse // don't reverse if list is singular, avoid excessive object creation
      var identB = if ( otherIdentity.tail == Nil ) otherIdentity else otherIdentity.reverse // don't reverse if list is singular, avoid excessive object creation
      var startOver = false
      var result = 0
      while ( result == 0 && ! startOver ) {
        identA match {
          case headA :: tailA => identB match {
            case headB :: tailB =>
              if ( headA == headB ) {
                identA = tailA
                identB = tailB
              }
              else result = if ( headA < headB ) -1 else 1
            case Nil =>
              val nextHashCode = System.identityHashCode( otherIdentity )
              val nextOtherIdentity = nextHashCode :: otherIdentity
              if ( other.intrinsicIdentity.compareAndSet( otherIdentity, nextOtherIdentity ) ) {
                otherIdentity = nextOtherIdentity
                identB = nextHashCode :: Nil
              } else startOver = true // bumped into another thread updating identity
          }
          case Nil =>
            val nextHashCode = System.identityHashCode( identity )
            val nextIdentity = nextHashCode :: identity
            if ( intrinsicIdentity.compareAndSet( identity, nextIdentity ) ) {
              identity = nextIdentity
              identA = nextHashCode :: Nil
            } else startOver = true // bumped into another thread updating identity
        }
      }
      if ( startOver )
        intrinsicCompareTo0( other )
      else result
    }    
  }
}