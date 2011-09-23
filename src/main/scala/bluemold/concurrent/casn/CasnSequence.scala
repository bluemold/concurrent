package bluemold.concurrent.casn

import org.bluemold.unsafe.Unsafe
import bluemold.concurrent._
import annotation.tailrec
import Unsafe._

object CasnVar {
  val valueIndex = objectDeclaredFieldOffset( classOf[CasnVar[Any]], "value" )
  val lockValueIndex = objectDeclaredFieldOffset( classOf[CasnVar[Any]], "lockValue" )
}
class CasnVar[T]( _initial: T ) {
  import CasnVar._
  @volatile var value: TaggedValue[T] = new TaggedValue[T]( _initial )
  @volatile var lockValue: CasnLock = new CasnLock( null, null, false )

  @inline
  private[casn] def getLockValue = lockValue

  @inline
  private[casn] def updateTagged( expect: TaggedValue[T], update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, valueIndex, expect, update )

  @inline
  private[casn] def updateLockValue( expect: CasnLock, update: CasnLock ): Boolean =
    Unsafe.compareAndSwapObject( this, lockValueIndex, expect, update )
  
  def getValue: T = value.value
  def getTagged: TaggedValue[T] = value
  def safeGet(): T = new CasnSequence( new GetOp( null, this ) ).executeAndGetLast()
  def safeGetTagged(): TaggedValue[T] = new CasnSequence( new GetOp( null, this ) ).executeAndGetLastTagged()
  def safeSet( value: T ) { new CasnSequence( new UpdateOp( null, this, ( old: T ) => value ) ).execute() }
  def safeCas( expect: T, update: T ): Boolean = new CasnSequence( new GenericOp( null, this, (op: CasnOp[T]) => expect, (op: CasnOp[T]) => update ) ).execute()
}

class TaggedValue[T]( _value: T ) {
  val value = _value
}

private[casn] final class CasnLock( _sequence: CasnSequence[_], _next: CasnLock, _isPreLock: Boolean ) {
  @inline def sequence = _sequence
  @inline def next = _next
  @inline def isPreLock = _isPreLock
  @inline def copy() = new CasnLock( _sequence, _next, _isPreLock )
}

private[casn] class CasnSeqStatus
private[casn] case object CasnSeqUndecided extends CasnSeqStatus
private[casn] case object CasnSeqPreLocked extends CasnSeqStatus
private[casn] case object CasnSeqLocked extends CasnSeqStatus
private[casn] case object CasnSeqSuccess extends CasnSeqStatus
private[casn] case object CasnSeqSuccessReleased extends CasnSeqStatus
private[casn] case object CasnSeqAborted extends CasnSeqStatus
private[casn] case object CasnSeqFailure extends CasnSeqStatus
private[casn] case object CasnSeqFailureReleased extends CasnSeqStatus

private[casn] class CasnOpStatus
private[casn] case object CasnOpUndecided extends CasnOpStatus
private[casn] case object CasnOpSuccess extends CasnOpStatus
private[casn] case object CasnOpFailure extends CasnOpStatus
private[casn] case object CasnOpReverted extends CasnOpStatus

private[casn] object CasnOp {
  val prevValueIndex = objectDeclaredFieldOffset( classOf[CasnOp[_]], "prevValue" )
}
sealed abstract class CasnOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T] ) {
  import CasnOp._
  final val target = _target
  final val prevOp = _prevOp
  @volatile var prevValue: TaggedValue[T] = null

  @inline final def getPrevValue: TaggedValue[T] = prevValue
  @inline def isReadOnly = true
  @inline def getUpdateValue = prevValue
  @inline def getRevertValue = prevValue

  @inline
  private[casn] def execute( sequence: CasnSequence[_] ): CasnOpStatus

  @inline
  private[casn] def revert( sequence: CasnSequence[_] ): CasnOpStatus

  @inline
  private[casn] final def executeSingleRead(): Boolean = setPrevValue( target.value )

  @inline
  private[casn] def checkExpectedValue( prevValue: TaggedValue[T] ): Boolean = true
  
  @inline
  private[casn] final def singleCheckExpectedValue(): Boolean = checkExpectedValue( prevValue )

  @inline
  private[casn] final def setPrevValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, prevValueIndex, null, update )

  @inline
  final def prior(index: Int): Any = prior( this, index )

  @inline
  @tailrec
  final def prior( op: CasnOp[_], index: Int ): Any = {
    if ( index < 0 ) null
    else if ( index == 0 ) {
      val prevValue = op.prevValue
      if ( prevValue != null ) prevValue.value
      else null
    } else {
      val prevOp = op.prevOp
      if ( prevOp != null ) prior( prevOp, index - 1 )
      else null
    }
  } 

  def execute(): Boolean = new CasnSequence( this ).execute();

  def executeAndGetLast(): T = new CasnSequence( this ).executeAndGetLast()

  def executeOption(): Option[T] = new CasnSequence( this ).executeOption()

  def executeAndGetLastTagged(): TaggedValue[T] = new CasnSequence( this ).executeAndGetLastTagged()

  // Only meant to be called by a single thread prior to
  // the sequence being added to the concurrent environment
  // via the execute method by that same thread

  @inline
  private final def addOp[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ) =
    new GenericOp( this, target, expect, update )

  def get[U]( target: CasnVar[U] ): CasnOp[U] = new GetOp( this, target )

  def set[U]( target: CasnVar[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    addOp( target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  def expect[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U ): CasnOp[U] =
    addOp( target, expect, ( op: CasnOp[U] ) => op.prevValue.value )

  def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ): CasnOp[U] =
    addOp( target, expect, update )

  def setVal[U]( target: CasnVar[U], update: U ): CasnOp[U] =
    addOp( target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

  def expectVal[U]( target: CasnVar[U], expect: U ): CasnOp[U] =
    addOp( target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )

  def casVal[U]( target: CasnVar[U], expect: U, update: U ): CasnOp[U] =
    addOp( target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )

  def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new ExpectTaggedGenericOp( this, target, expect, update )

  def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ): CasnOp[U] =
    new ExpectTaggedUpdateOp( this, target, expect, update )

  def expectTagged[U]( target: CasnVar[U], expect: TaggedValue[U] ): CasnOp[U] =
    new ExpectTaggedOp( this, target, expect )

  def update[U]( target: CasnVar[U], update: ( U ) => U ): CasnOp[U] =
    new UpdateOp( this, target, update )
}

private[casn] object CasnModifyOp {
  val updateValueIndex = objectDeclaredFieldOffset( classOf[CasnModifyOp[_]], "updateValue" ) 
  val revertValueIndex = objectDeclaredFieldOffset( classOf[CasnModifyOp[_]], "revertValue" ) 
}

sealed abstract class CasnModifyOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T] ) extends CasnOp[T]( _prevOp, _target ) {
  import CasnModifyOp._
  @volatile var updateValue: TaggedValue[T] = null
  @volatile var revertValue: TaggedValue[T] = null

  @inline
  override final def isReadOnly = false
  @inline
  override final def getUpdateValue = updateValue
  @inline
  override final def getRevertValue = revertValue

  @inline
  private[casn] def generateUpdateValue( prevValue: TaggedValue[T] ): TaggedValue[T]
  
  @inline
  private[casn] final def setUpdateValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, updateValueIndex, null, update )

  @inline
  private[casn] final def setRevertValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, revertValueIndex, null, update )

  @inline
  override final def execute( sequence: CasnSequence[_] ) = executeTR( sequence )
  @inline
  override final def revert( sequence: CasnSequence[_] ) = revertTR( sequence )

  @inline
  @tailrec
  private final def executeTR( sequence: CasnSequence[_] ): CasnOpStatus = {
    val prevValue = this.prevValue
    if ( prevValue == null ) {
      val prevValue = target.getTagged
      if ( setPrevValue( prevValue ) ) {
        if ( checkExpectedValue( prevValue ) ) {
          val updateValue = generateUpdateValue( prevValue )
          if ( target.updateTagged( prevValue, updateValue ) ) {
            if ( setUpdateValue( updateValue ) ) CasnOpSuccess
            else executeTR( sequence ) // try again
          } else executeTR( sequence ) // try again
        } else CasnOpFailure // did not pass expected
      } else executeTR( sequence ) // try again
    } else { // prevValue already set
      val updateValue = this.updateValue
      if ( updateValue == null ) {
        val revertValue = this.revertValue
        if ( revertValue == null ) {
          if ( checkExpectedValue( prevValue ) ) {
            val updateValue = generateUpdateValue( prevValue )
            if ( target.updateTagged( prevValue, updateValue ) ) {
              if ( setUpdateValue( updateValue ) ) CasnOpSuccess
              else executeTR( sequence ) // try again
            } else executeTR( sequence ) // try again
          } else {
            if ( setRevertValue( prevValue ) ) CasnOpFailure
            else executeTR( sequence ) // try again
          } // did not pass expected
        } else CasnOpFailure
      } else CasnOpSuccess // execute is already successful
    }
  }

  @inline
  @tailrec
  private final def revertTR( sequence: CasnSequence[_] ): CasnOpStatus = {
    val updateValue = this.updateValue
    if ( updateValue == null ) {
      throw new RuntimeException( "What Happened" )
    } else {
      val revertValue = this.revertValue
      if ( revertValue == null ) {
        val currentValue = target.getTagged
        if ( sequence.revertOp != null ) {
          val revertValue = new TaggedValue( prevValue.value )
          if ( target.updateTagged( currentValue, revertValue ) ) {
            if ( setRevertValue( revertValue ) ) CasnOpReverted
            else revertTR( sequence ) // unexpected
          } else revertTR( sequence ) // unexpected
        } else CasnOpReverted
      } else CasnOpReverted
    }
  }
} 

object NoOp extends CasnOp[Nothing]( null, null ) {
  private[casn] def execute(sequence: CasnSequence[_]) = CasnOpFailure 
  private[casn] def revert(sequence: CasnSequence[_]) = CasnOpReverted

  override def get[U]( target: CasnVar[U] ): CasnOp[U] = new GetOp( null, target )

  override def set[U]( target: CasnVar[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  override def expect[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( null, target, expect, ( op: CasnOp[U] ) => op.prevValue.value )

  override def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( null, target, expect, update )

  override def setVal[U]( target: CasnVar[U], update: U ): CasnOp[U] =
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

  override def expectVal[U]( target: CasnVar[U], expect: U ): CasnOp[U] =
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )

  override def casVal[U]( target: CasnVar[U], expect: U, update: U ): CasnOp[U] =
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )

  override def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new ExpectTaggedGenericOp( null, target, expect, update )

  override def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ): CasnOp[U] =
    new ExpectTaggedUpdateOp( null, target, expect, update )

  override def expectTagged[U]( target: CasnVar[U], expect: TaggedValue[U] ): CasnOp[U] =
    new ExpectTaggedOp( null, target, expect )

  override def update[U]( target: CasnVar[U], update: ( U ) => U ): CasnOp[U] =
    new UpdateOp( null, target, update )
}

private[casn] final class GenericOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T], expect: ( CasnOp[T] ) => T, update: ( CasnOp[T] ) => T ) extends CasnModifyOp[T]( _prevOp, _target ) {
  @inline
  override def generateUpdateValue( prevValue: TaggedValue[T] ): TaggedValue[T] = new TaggedValue[T]( update( this ) )

  @inline
  override def checkExpectedValue( prevValue: TaggedValue[T] ): Boolean = prevValue.value == expect( this )
}

private[casn] final class GetOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T] ) extends CasnOp[T]( _prevOp, _target ) {
  @inline
  override def execute( sequence: CasnSequence[_] ) = executeTR( sequence )
  private def executeTR( sequence: CasnSequence[_] ): CasnOpStatus = {
    val prevValue = this.prevValue
    if ( prevValue == null ) {
      if ( setPrevValue( target.getTagged ) ) CasnOpSuccess
      else executeTR( sequence )
    } else CasnOpSuccess
  }

  @inline
  override def revert( sequence: CasnSequence[_] ) = CasnOpReverted
}

private[casn] final class ExpectTaggedOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T], expectTagged: TaggedValue[T] ) extends CasnOp[T]( _prevOp, _target ) {
  @inline
  override def execute( sequence: CasnSequence[_] ) = executeTR( sequence )
  @inline
  override def revert( sequence: CasnSequence[_] ) = CasnOpReverted

  @inline
  override private[casn] def checkExpectedValue( prevValue: TaggedValue[T] ) = expectTagged == prevValue

  @inline
  @tailrec
  private def executeTR( sequence: CasnSequence[_] ): CasnOpStatus = {
    val prevValue = this.prevValue
    if ( prevValue == null ) {
      val currentValue = target.getTagged
      if ( setPrevValue( currentValue ) ) {
        if ( currentValue == expectTagged ) CasnOpSuccess 
        else CasnOpFailure
      } else executeTR( sequence ) // try again
    } else if ( prevValue == expectTagged ) CasnOpSuccess 
    else CasnOpFailure
  }
}

private[casn] final class UpdateOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T], update: ( T ) => T ) extends CasnModifyOp[T]( _prevOp, _target ) {
  @inline
  override private[casn] def generateUpdateValue( prevValue: TaggedValue[T] ) = new TaggedValue( update( prevValue.value ) )
}

private[casn] final class ExpectTaggedUpdateOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T], expect: TaggedValue[T], update: T ) extends CasnModifyOp[T]( _prevOp, _target ) {
  @inline
  override private[casn] def generateUpdateValue( prevValue: TaggedValue[T] ) = new TaggedValue( update )
  @inline
  override private[casn] def checkExpectedValue( prevValue: TaggedValue[T] ) = prevValue == expect
}

private[casn] final class ExpectTaggedGenericOp[T]( _prevOp: CasnOp[_], _target: CasnVar[T], expect: TaggedValue[T], update: ( CasnOp[T] ) => T ) extends CasnModifyOp[T]( _prevOp, _target ) {
  @inline
  override private[casn] def generateUpdateValue( prevValue: TaggedValue[T] ) = new TaggedValue( update( this ) )
  @inline
  override private[casn] def checkExpectedValue( prevValue: TaggedValue[T] ) = prevValue == expect
}

private[casn] final class CasnProxyOp( _op: CasnOp[_], _nextOp: CasnProxyOp ) {
  def op = _op
  def nextOp = _nextOp
} 

object CasnSequence {
  private[casn] val uniqueIdentityIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "uniqueIdentity" ) 
  private[casn] val sequenceStatusIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "status" ) 
  private[casn] val sequencePreLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "preLockOp" ) 
  private[casn] val sequenceLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "lockOp" ) 
  private[casn] val sequenceUpdateOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "updateOp" ) 
  private[casn] val sequenceRevertOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "revertOp" ) 
  private[casn] val sequenceReleaseOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence[Any]], "releaseOp" ) 

  def get[U]( target: CasnVar[U] ) = new GetOp( null, target )

  def set[U]( target: CasnVar[U], update: ( CasnOp[U] ) => U ) =
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  def expect[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U ) =
    new GenericOp( null, target, expect, ( op: CasnOp[U] ) => op.prevValue.value )

  def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ) =
    new GenericOp( null, target, expect, update )

  def setVal[U]( target: CasnVar[U], update: U ) =
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

  def expectVal[U]( target: CasnVar[U], expect: U ) =
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )

  def casVal[U]( target: CasnVar[U], expect: U, update: U ) =
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )

  def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new ExpectTaggedGenericOp( null, target, expect, update )

  def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ): CasnOp[U] =
    new ExpectTaggedUpdateOp( null, target, expect, update )

  def expectTagged[U]( target: CasnVar[U], expect: TaggedValue[U] ) =
    new ExpectTaggedOp( null, target, expect )

  def update[U]( target: CasnVar[U], update: ( U ) => U ) =
    new UpdateOp( null, target, update )

  @inline
  @tailrec
  private[casn] def getFirstOp( lastOp: CasnOp[_] ): CasnOp[_] = {
    val prevOp = lastOp.prevOp
    if ( prevOp == null ) lastOp else getFirstOp( prevOp ) 
  }

  @inline
  @tailrec
  private[casn] def isReadOnly( lastOp: CasnOp[_] ): Boolean = {
    val prevOp = lastOp.prevOp
    if ( prevOp == null ) lastOp.isReadOnly
    else if ( lastOp.isReadOnly ) isReadOnly( prevOp )
    else false
  }

  @inline
  @tailrec
  private[casn] def getProxyOp( lastOp: CasnOp[_], nextProxyOp: CasnProxyOp ): CasnProxyOp = {
    if ( lastOp == null ) nextProxyOp
    else getProxyOp( lastOp.prevOp, new CasnProxyOp( lastOp, nextProxyOp ) )
  }

  private[casn] val blockingTestCounter = new AtomicLong
  def getBlockingTestCount = blockingTestCounter.get()
}
final class CasnSequence[T]( lastOp: CasnOp[T] ) {
  import CasnSequence._

  @volatile var uniqueIdentity: List[Int] = Nil

  val isSingleOp = lastOp.prevOp == null
  val readOnly: Boolean = isReadOnly( lastOp )

  val firstProxyOp: CasnProxyOp = if ( isSingleOp ) null else getProxyOp( lastOp, null )

  @volatile var status: CasnSeqStatus = CasnSeqUndecided
  @volatile var preLockOp: CasnProxyOp = firstProxyOp
  @volatile var lockOp: CasnProxyOp = firstProxyOp
  @volatile var updateOp: CasnProxyOp = firstProxyOp
  @volatile var revertOp: CasnOp[_] = lastOp
  @volatile var releaseOp: CasnOp[_] = lastOp

  @inline
  def getLastOp = lastOp
  
  @inline
  private def updateUniqueIdentity( expect: List[Int], update: List[Int] ): Boolean =
    Unsafe.compareAndSwapObject( this, uniqueIdentityIndex, expect, update )

  @inline
  private def setNextPreLockOp( expect: CasnProxyOp, update: CasnProxyOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequencePreLockOpIndex, expect, update )

  @inline
  private def setNextLockOp( expect: CasnProxyOp, update: CasnProxyOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceLockOpIndex, expect, update )

  @inline
  private def setNextUpdateOp( expect: CasnProxyOp, update: CasnProxyOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceUpdateOpIndex, expect, update )

  @inline
  private def setNextRevertOp( expect: CasnOp[_], update: CasnOp[_] ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceRevertOpIndex, expect, update )

  @inline
  private def setNextReleaseOp( expect: CasnOp[_], update: CasnOp[_] ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceReleaseOpIndex, expect, update )

  @inline
  private def updateStatus( expect: CasnSeqStatus, update: CasnSeqStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceStatusIndex, expect, update )

  @inline
  private def hasPriorityOver( other: CasnSequence[_] ): Boolean = {
    if ( hashCode == other.hashCode ) hasPriorityOverByUniqueIdentity( other )
    else hashCode < other.hashCode
  }

  @inline
  private def hasPriorityOverByUniqueIdentity( other: CasnSequence[_] ): Boolean = {
    var identity = uniqueIdentity
    var otherIdentity = other.uniqueIdentity
    if ( identity == Nil ) {
      updateUniqueIdentity( identity, lastOp.hashCode() :: Nil )
      if ( otherIdentity == Nil )
        other.updateUniqueIdentity( otherIdentity, other.getLastOp.hashCode() :: Nil )
      hasPriorityOverByUniqueIdentity( other )
    } else if ( otherIdentity == Nil ) {
      other.updateUniqueIdentity( otherIdentity, other.getLastOp.hashCode() :: Nil )
      hasPriorityOverByUniqueIdentity( other )
    } else {
      var identA = identity.reverse
      var identB = otherIdentity.reverse
      var startOver = false
      var notDone = true
      var result = false
      while ( notDone && ! startOver ) {
        identA match {
          case headA :: tailA => identB match {
            case headB :: tailB =>
              if ( headA == headB ) {
                identA = tailA
                identB = tailB
              }
              else {
                result = headA < headB
                notDone = false
              }
            case Nil =>
              val nextHashCode = System.identityHashCode( otherIdentity )
              val nextOtherIdentity = nextHashCode :: otherIdentity
              if ( other.updateUniqueIdentity( otherIdentity, nextOtherIdentity ) ) {
                otherIdentity = nextOtherIdentity
                identB = nextHashCode :: Nil
              } else startOver = true // bumped into another thread updating identity
          }
          case Nil =>
            val nextHashCode = System.identityHashCode( identity )
            val nextIdentity = nextHashCode :: identity
            if ( updateUniqueIdentity( identity, nextIdentity ) ) {
              identity = nextIdentity
              identA = nextHashCode :: Nil
            } else startOver = true // bumped into another thread updating identity
        }
      }
      if ( startOver )
        hasPriorityOverByUniqueIdentity( other )
      else result
    }
  }

  def execute(): Boolean = process( this )

  def executeAndGetLast(): T = {
    if ( process( this ) ) {
      val prevValue = lastOp.getPrevValue
      if ( prevValue != null ) prevValue.value
      else throw new IllegalStateException( "sequence did not complete successfully" )
    } else throw new IllegalStateException( "sequence did not complete successfully" )
  }

  def executeOption(): Option[T] = {
    if ( process( this ) ) {
      val lastValue = lastOp.getUpdateValue
      if ( lastValue != null ) Some( lastValue.value ) else None
    } else None
  }

  def executeAndGetLastTagged(): TaggedValue[T] = {
    if ( process( this ) ) {
      val prevValue = lastOp.getPrevValue
      if ( prevValue != null ) prevValue
      else throw new IllegalStateException( "sequence did not complete successfully" )
    } else throw new IllegalStateException( "sequence did not complete successfully" )
  }

  @inline
  private def process( sequence: CasnSequence[_] ): Boolean = processTR( Nil, sequence )

  @inline
  private def updatePreLock( sequence: CasnSequence[_], op: CasnProxyOp, target: CasnVar[_], currentLock: CasnLock ) {
    if ( sequence.status == CasnSeqUndecided )
      if ( target.updateLockValue( currentLock, new CasnLock( sequence, currentLock, true ) ) )
        preLockingNextStep( sequence, op )
  }

  @inline
  private def preLockingNextStep( sequence: CasnSequence[_], op: CasnProxyOp ) {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextPreLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqUndecided, CasnSeqPreLocked )
  }

  @inline
  private def updateLock( sequence: CasnSequence[_], op: CasnProxyOp, target: CasnVar[_], currentLock: CasnLock ) {
    if ( sequence.status == CasnSeqPreLocked )
      if ( target.updateLockValue( currentLock, new CasnLock( sequence, currentLock.next, false ) ) )
        lockingNextStep( sequence, op )
  }

  @inline
  private def lockingNextStep( sequence: CasnSequence[_], op: CasnProxyOp ) {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqPreLocked, CasnSeqLocked )
//    else sequence.updateStatus( CasnSeqPreLocked, CasnSeqSuccess )
  }

  @inline
  private def updatingNextStep( sequence: CasnSequence[_], op: CasnProxyOp ) {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextUpdateOp( op, nextOp )
    else sequence.updateStatus( CasnSeqLocked, CasnSeqSuccess )
  }

  @inline
  private def revertingNextStep( sequence: CasnSequence[_], op: CasnOp[_] ) {
    val prevOp = op.prevOp
    if ( prevOp != null ) setNextRevertOp( op, prevOp )
    else sequence.updateStatus( CasnSeqAborted, CasnSeqFailure )
  }

  @inline
  private def releasingNextStep( sequence: CasnSequence[_], sequenceStatus: CasnSeqStatus, op: CasnOp[_] ) {
    val prevOp = op.prevOp
    if ( prevOp != null ) setNextReleaseOp( op, prevOp )
    else if ( sequenceStatus == CasnSeqSuccess ) sequence.updateStatus( CasnSeqSuccess, CasnSeqSuccessReleased )
    else sequence.updateStatus( CasnSeqFailure, CasnSeqFailureReleased )
  }

  @tailrec
  private def processTR( sequences: List[CasnSequence[_]], sequence: CasnSequence[_] ): Boolean = {
    val sequenceStatus = sequence.status
    sequenceStatus match {
      case CasnSeqUndecided => { // PreLocking
        if ( sequence.isSingleOp ) {
          if ( sequence.readOnly ) { // Single ReadOnly Op
            val op = sequence.getLastOp
            val target = op.target
            val currentLock = target.lockValue
            val currentLockSequence = currentLock.sequence
            if ( currentLockSequence == null ) {
              val prevValue = op.prevValue
              if ( prevValue == null ) {
                // not currently locked or pre locked, attempt lock because we are a single sequence
                val newLock = new CasnLock( sequence, currentLock, false )
                if ( target.updateLockValue( currentLock, newLock ) ) {
                  if ( op.executeSingleRead() ) {
                    val postLock = currentLock.copy()
                    if ( target.updateLockValue( newLock, postLock ) ) {
                      sequences match {
                        case head :: tail => processTR( tail, head )
                        case Nil => op.singleCheckExpectedValue()
                      }
                    } else processTR( sequences, sequence ) // try again
                  } else processTR( sequences, sequence ) // try again
                } else processTR( sequences, sequence ) // try again
              } else sequences match { // We already succeeded
                case head :: tail => processTR( tail, head )
                case Nil => op.singleCheckExpectedValue()
              }
            } else {
              if ( sequence == currentLockSequence ) { // We have it locked
                val prevValue = op.prevValue
                if ( prevValue == null ) {
                  if ( op.executeSingleRead() ) {
                    val postLock = currentLock.next.copy()
                    if ( target.updateLockValue( currentLock, postLock ) ) {
                      sequences match {
                        case head :: tail => processTR( tail, head )
                        case Nil => op.singleCheckExpectedValue()
                      }
                    } else processTR( sequences, sequence ) // try again
                  } else processTR( sequences, sequence ) // try again
                } else {
                  val postLock = currentLock.next.copy()
                  if ( target.updateLockValue( currentLock, postLock ) ) {
                    sequences match {
                      case head :: tail => processTR( tail, head )
                      case Nil => true
                    }
                  } else processTR( sequences, sequence ) // try again
                }
              } else { // Someone else has it locked
                val prevValue = op.prevValue
                if ( prevValue == null ) {
                  val newSequences = sequence :: sequences
                  processTR( newSequences, currentLockSequence ) // help out
                } else sequences match { // We already succeeded
                  case head :: tail => processTR( tail, head )
                  case Nil => op.singleCheckExpectedValue()
                }
              } 
            }
          } else { // Single Modify Op
            val op = sequence.getLastOp
            val target = op.target
            val currentLock = target.getLockValue
            val currentLockSequence = currentLock.sequence
            if ( currentLockSequence == null ) {
              if ( op.getUpdateValue == null ) {
                if ( op.getRevertValue == null ) {
                  // not currently locked or pre locked, attempt lock because we are a single sequence
                  val newLock = new CasnLock( sequence, currentLock, false )
                  if ( target.updateLockValue( currentLock, newLock ) ) {
                    val opStatus = op.execute( sequence )
                    val postLock = currentLock.copy()
                    if ( target.updateLockValue( newLock, postLock ) ) {
                      sequences match { // Completed
                        case head :: tail => processTR( tail, head )
                        case Nil => opStatus == CasnOpSuccess
                      }                      
                    } else processTR( sequences, sequence ) // try again                    
                  } else processTR( sequences, sequence ) // try again
                } else {
                  sequences match { // Completed
                    case head :: tail => processTR( tail, head )
                    case Nil => false
                  }                      
                }
              } else {
                sequences match { // Completed
                  case head :: tail => processTR( tail, head )
                  case Nil => true
                }                      
              }
            } else { 
              if ( sequence == currentLockSequence ) { // We have the lock already
                val opStatus = op.execute( sequence )
                val postLock = currentLock.next.copy()
                if ( target.updateLockValue( currentLock, postLock ) ) {
                  sequences match { // Completed
                    case head :: tail => processTR( tail, head )
                    case Nil => opStatus == CasnOpSuccess
                  }                      
                } else processTR( sequences, sequence ) // try again                    
              } else { // Someone else has the lock
                if ( op.getUpdateValue == null ) {
                  if ( op.getRevertValue == null ) {
                    val newSequences = sequence :: sequences
                    processTR( newSequences, currentLockSequence ) // help out
                  } else {
                    sequences match { // Completed
                      case head :: tail => processTR( tail, head )
                      case Nil => false
                    }                      
                  }
                } else {
                  sequences match { // Completed
                    case head :: tail => processTR( tail, head )
                    case Nil => true
                  }                      
                }
              }
            }
          }
        } else { // Not a Single Op
          val proxyOp = sequence.preLockOp
          val target = proxyOp.op.target
          val currentLock = target.getLockValue
          val currentLockSequence = currentLock.sequence
          if ( currentLockSequence == null ) {
            // not currently locked or pre locked, attempt pre lock
            updatePreLock( sequence, proxyOp, target, currentLock )
            processTR( sequences, sequence )
          } else { 
            if ( sequence == currentLockSequence || onLockChain( currentLock.next, sequence ) ) {
              // already ( prelocked or locked ) move on to the next step
              preLockingNextStep( sequence, proxyOp )
              processTR( sequences, sequence )
            } else if ( currentLock.isPreLock ) {
              // currently pre locked
              if ( sequence.hasPriorityOver( currentLockSequence ) ) {
                // we have priority, grab the pre lock
                updatePreLock( sequence, proxyOp, target, currentLock )
                processTR( sequences, sequence )
              } else {
                // someone with higher priority got here first so help them
//                  if ( sequences.contains( currentLockSequence ) )
//                    throw new RuntimeException( "This should not happen" )
//                  else {
                  val newSequences = sequence :: sequences
                  processTR( newSequences, currentLockSequence ) // help out
//                  } 
              }
            } else {
              // currently locked
              if ( proxyOp == sequence.firstProxyOp ) {
//                  if ( sequences.contains( currentLockSequence ) )
//                    throw new RuntimeException( "This should not happen" )
//                  else {
                  val newSequences = sequence :: sequences
                  processTR( newSequences, currentLockSequence ) // help out
//                  } 
              }
              else { // we might be blocking
                if ( sequence.hasPriorityOver( currentLockSequence ) ) {
                  if ( areWeBlockingTargetSequence( sequence, currentLockSequence ) ) {
                    // grab the pre lock
                    updatePreLock( sequence, proxyOp, target, currentLock )
                    processTR( sequences, sequence )
                  } else {
                    // we are not blocking the lock, try to help out
//                      if ( sequences.contains( currentLockSequence ) )
//                        throw new RuntimeException( "This should not happen" )
//                      else {
                      val newSequences = sequence :: sequences
                      processTR( newSequences, currentLockSequence ) // help out
//                      }
                  }
                } else {
                  // we don't have priority, try to help out
//                    if ( sequences.contains( currentLockSequence ) )
//                      throw new RuntimeException( "This should not happen" )
//                    else {
                    val newSequences = sequence :: sequences
                    processTR( newSequences, currentLockSequence ) // help out
//                    } 
                }
              }
            }
          }
        }
      }
      case CasnSeqPreLocked => { // Locking
        val proxyOp = sequence.lockOp
        val target = proxyOp.op.target
        val currentLock = target.getLockValue
        val currentLockSequence = currentLock.sequence
        if ( currentLockSequence == null )
          processTR( sequences, sequence ) // this can happen, re-run step
        else {
          if ( sequence == currentLockSequence ) { // if we have an immediate lock on it
            if ( currentLock.isPreLock ) { // and we have a pre lock then attempt to replace pre lock with lock
              updateLock( sequence, proxyOp, target, currentLock )
            } else lockingNextStep( sequence, proxyOp ) // otherwise already locked, move on to the next one
            processTR( sequences, sequence ) // continue
          } else {
            if ( onLockChain( currentLock.next, sequence ) ) {
              // we should have a pre lock on the chain, and someone with higher priority must have bumped us so help them
              if ( sequences.contains( currentLockSequence ) )
                throw new RuntimeException( "This should not happen" )
              else {
                val newSequences = sequence :: sequences
                processTR( newSequences, currentLockSequence ) // help out
              } 
            } else processTR( sequences, sequence ) // not on chain, this is unexpected
          }
        }
      }
      case CasnSeqLocked => { // Updating
        val proxyOp = sequence.updateOp
        val op = proxyOp.op
//        val currentLock = op.target.getLockValue
//        val currentLockSequence = currentLock.sequence
        op.execute( sequence ) match {
          case CasnOpSuccess =>
            updatingNextStep( sequence, proxyOp )
            processTR( sequences, sequence )
          case CasnOpFailure =>
            val prevOp = op.prevOp
            if ( prevOp == null ) {
              sequence.updateStatus( CasnSeqLocked, CasnSeqFailure )
              processTR( sequences, sequence )
            } else {
              setNextRevertOp( null, prevOp )
              sequence.updateStatus( CasnSeqLocked, CasnSeqAborted )
              processTR( sequences, sequence )
            }
        }
      }
      case CasnSeqAborted => { // Reverting
        val op = sequence.revertOp
        val opStatus = op.revert( sequence ) // this should set the op status, if not then there was an unexpected state
        if ( opStatus == CasnOpReverted ) {
          revertingNextStep( sequence, op )
          processTR( sequences, sequence )
        } else throw new RuntimeException( "What Happened!" )
      }
      case CasnSeqSuccess | CasnSeqFailure => { // Releasing
        val op = sequence.releaseOp
        val target = op.target
        val currentLock = target.getLockValue
        val currentLockSequence = currentLock.sequence
        if ( currentLockSequence == sequence ) {
          if ( target.updateLockValue( currentLock, currentLock.next.copy() ) )
            releasingNextStep( sequence, sequenceStatus, op )
        } else if ( onLockChain( currentLock, sequence ) )
          throw new RuntimeException( "This shouldn't happen" )
        else releasingNextStep( sequence, sequenceStatus, op )
        processTR( sequences, sequence )
      }
      case CasnSeqSuccessReleased | CasnSeqFailureReleased => sequences match {
        case head :: tail => processTR( tail, head )
        case Nil => sequence.status == CasnSeqSuccessReleased
      }
      case _ => throw new RuntimeException( "What Happened!" ) 
    }
  }

  @inline
  @tailrec
  private def onLockChain( lock: CasnLock, sequence: CasnSequence[_] ): Boolean = {
    if ( lock == null ) false
    else if ( lock.sequence == sequence ) true
    else onLockChain( lock.next, sequence )
  }

  @inline
  private def areWeBlockingTargetSequence( sequence: CasnSequence[_], targetSequence: CasnSequence[_] ): Boolean = {
    blockingTestCounter.incrementAndGet()
    val lastOp = targetSequence.getLastOp
    areWeBlockingTargetSequenceLock( Nil, sequence, targetSequence, lastOp, lastOp.target.getLockValue )
  }

  @inline
  @tailrec
  private def areWeBlockingTargetSequenceLock( sequences: List[CasnSequence[_]], sequence: CasnSequence[_], targetSequence: CasnSequence[_], op: CasnOp[_], lock: CasnLock ): Boolean = {
    if ( lock == null ) false // something has changed, break out with false
    else {
      val lockSequence = lock.sequence
      if ( lockSequence == null ) false // something has changed, break out with false 
      else if ( lockSequence == targetSequence ) {
        val prevOp = op.prevOp
        if ( prevOp == null ) sequences match {
          case Nil => false
          case head :: tail =>
            val lastOp = targetSequence.getLastOp
            areWeBlockingTargetSequenceLock( tail, sequence, head, lastOp, lastOp.target.getLockValue )
        }
        else areWeBlockingTargetSequenceLock( sequences, sequence, targetSequence, prevOp, prevOp.target.getLockValue )
      }
      else if ( lockSequence == sequence ) true
      else if ( sequences.contains( lockSequence ) )
        areWeBlockingTargetSequenceLock( sequences, sequence, targetSequence, op, lock.next )
      else areWeBlockingTargetSequenceLock( lockSequence :: sequences, sequence, targetSequence, op, lock.next )
    }
  }
}
