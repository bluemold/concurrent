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
  @volatile var lockValue: CasnLockValue = new CasnLockValue( null )

  @inline
  private[casn] def getLockValue = lockValue

  @inline
  private[casn] def updateTagged( expect: TaggedValue[T], update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, valueIndex, expect, update )

  @inline
  private[casn] def updateLockValue( expect: CasnLockValue, update: CasnLockValue ): Boolean =
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
private[casn] class CasnLockValue( _lock: CasnLock ) {
  val lock = _lock
}
private[casn] case class CasnLock( target: CasnVar[_], sequence: CasnSequence[_], next: CasnLock, isPreLock: Boolean )

private[casn] class CasnSeqStatus
private[casn] case object CasnSeqConstructing extends CasnSeqStatus
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
  val opStatusIndex = objectDeclaredFieldOffset( classOf[CasnOp[_]], "opStatus" ) 
  val prevValueIndex = objectDeclaredFieldOffset( classOf[CasnOp[_]], "prevValue" )

  @tailrec
  def isTargetPreviouslyLocked( prevOp: CasnOp[_], target: CasnVar[_] ): Boolean =
    if ( prevOp == null ) false
    else if ( prevOp.getTarget == target ) true
    else isTargetPreviouslyLocked( prevOp.prevOp, target )
}
abstract class CasnOp[T]( _prevOp: CasnOp[_] ) {
  import CasnOp._
  @volatile var opStatus: CasnOpStatus = CasnOpUndecided
  @volatile var prevValue: TaggedValue[T] = null
  @volatile var nextOp: CasnOp[_] = null

  @volatile var targetPreviouslyLocked = false
  @volatile var prevOp = _prevOp
  if ( _prevOp != null )
    _prevOp.nextOp = this

  def initialize() {
    targetPreviouslyLocked = isTargetPreviouslyLocked( prevOp, getTarget )
  }


  final def getPrevValue: TaggedValue[T] = prevValue
  def isReadOnly = true
  def getUpdateValue = prevValue
  def unlink() { nextOp = null }

  def getTarget: CasnVar[T]
  private[casn] def execute( sequence: CasnSequence[_] )
  private[casn] def revert( sequence: CasnSequence[_] )

  @inline
  private[casn] final def executeSingleRead(): Boolean = setPrevValue( getTarget.value )

  private[casn] def evaluateSingleExpect(): Boolean = true

  @inline
  private[casn] final def updateOpStatus( expect: CasnOpStatus, update: CasnOpStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, opStatusIndex, expect, update )

  @inline
  private[casn] final def setPrevValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, prevValueIndex, null, update )

  @inline
  @tailrec
  final def fail() {
    opStatus match {
      case CasnOpUndecided => if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) ) fail()
      case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
    }
  }

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

  private final def addOp[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ) =
      new GenericOp( this, target, expect, update )

  def get[U]( target: CasnVar[U] ): CasnOp[U] = new GetOp( this, target )

  def set[U]( target: CasnVar[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    addOp( target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  def expect[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U ): CasnOp[U] =
    addOp( target, expect, ( op: CasnOp[U] ) => op.prevValue.value )

  def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ): CasnOp[U] = {
    addOp( target, expect, update )
  }

  def setVal[U]( target: CasnVar[U], update: U ): CasnOp[U] = {
    addOp( target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )
  }

  def expectVal[U]( target: CasnVar[U], expect: U ): CasnOp[U] = {
    addOp( target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )
  }

  def casVal[U]( target: CasnVar[U], expect: U, update: U ): CasnOp[U] = {
    addOp( target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )
  }

  def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( new ExpectTaggedOp( this, target, expect ), target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ): CasnOp[U] =
    new GenericOp( new ExpectTaggedOp( this, target, expect ), target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

  def expectTagged[U]( target: CasnVar[U], expect: TaggedValue[U] ): CasnOp[U] =
    new ExpectTaggedOp( this, target, expect )

  def update[U]( target: CasnVar[U], update: ( U ) => U ): CasnOp[U] =
    new UpdateOp( this, target, update )
}

private[casn] object CasnModifyOp {
  val updateValueIndex = objectDeclaredFieldOffset( classOf[CasnModifyOp[_]], "updateValue" ) 
  val revertValueIndex = objectDeclaredFieldOffset( classOf[CasnModifyOp[_]], "revertValue" ) 

  @tailrec
  def setPriorModifyOpForTarget[T]( prevOp: CasnOp[_], target: CasnVar[T], currentOp: CasnModifyOp[T] ) {
    if ( prevOp != null ) {
      if ( prevOp.getTarget == target )
        prevOp match {
          case priorOp: CasnModifyOp[T] => priorOp.nextModifyOpForTarget = currentOp
          case _ => setPriorModifyOpForTarget( prevOp.prevOp, target, currentOp )
        }
      else setPriorModifyOpForTarget( prevOp.prevOp, target, currentOp )
    }
  }
}

abstract class CasnModifyOp[T]( _prevOp: CasnOp[_] ) extends CasnOp[T]( _prevOp ) {
  import CasnModifyOp._
  @volatile var updateValue: TaggedValue[T] = null
  @volatile var revertValue: TaggedValue[T] = null
  @volatile var nextModifyOpForTarget: CasnModifyOp[T] = null

  override final def isReadOnly = false
  override final def getUpdateValue = updateValue

  override def initialize() {
    super.initialize()
    if ( targetPreviouslyLocked )
      setPriorModifyOpForTarget( prevOp, getTarget, this )
  }

  override final def unlink() {
    nextOp = null
    nextModifyOpForTarget = null
  }

  final def getRevertValue = revertValue
  private[casn] def setUpdateValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, updateValueIndex, null, update )

  private[casn] def setRevertValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, revertValueIndex, null, update )
} 

object NoOp extends CasnOp[Nothing]( null ) {
  def getTarget = null

  private[casn] def execute(sequence: CasnSequence[_]) {}
  private[casn] def revert(sequence: CasnSequence[_]) {}

  override def get[U]( target: CasnVar[U] ): CasnOp[U] = new GetOp( null, target )

  override def set[U]( target: CasnVar[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  override def expect[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U ): CasnOp[U] =
    new GenericOp( null, target, expect, ( op: CasnOp[U] ) => op.prevValue.value )

  override def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ): CasnOp[U] = {
    new GenericOp( null, target, expect, update )
  }

  override def setVal[U]( target: CasnVar[U], update: U ): CasnOp[U] = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )
  }

  override def expectVal[U]( target: CasnVar[U], expect: U ): CasnOp[U] = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )
  }

  override def casVal[U]( target: CasnVar[U], expect: U, update: U ): CasnOp[U] = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )
  }

  override def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ): CasnOp[U] =
              new ExpectTaggedOp( null, target, expect )
              .cas( target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  override def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ): CasnOp[U] =
      new ExpectTaggedOp( null, target, expect )
      .cas( target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

  override def expectTagged[U]( target: CasnVar[U], expect: TaggedValue[U] ): CasnOp[U] =
    new ExpectTaggedOp( null, target, expect )

  override def update[U]( target: CasnVar[U], update: ( U ) => U ): CasnOp[U] =
    new UpdateOp( null, target, update )
}

private[casn] object GenericOp {
  val expectValueIndex = objectDeclaredFieldOffset( classOf[GenericOp[_]], "expectValue" ) 
}
private[casn] final class GenericOp[T]( _prevOp: CasnOp[_], target: CasnVar[T], expect: ( CasnOp[T] ) => T, update: ( CasnOp[T] ) => T ) extends CasnModifyOp[T]( _prevOp ) {
  import GenericOp._
  @volatile var expectValue: TaggedValue[T] = null

  private[casn] def setExpectValue( update: TaggedValue[T] ): Boolean =
    Unsafe.compareAndSwapObject( this, expectValueIndex, null, update )

  def getTarget = target
  override def execute( sequence: CasnSequence[_] ) { executeTR( sequence ) }
  override def revert( sequence: CasnSequence[_] ) { revertTR( sequence ) }

  override private[casn] def evaluateSingleExpect() = prevValue.value == expect( this )

  @inline
  @tailrec
  private def executeTR( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpUndecided => {
        val prevValue = this.prevValue
        if ( prevValue == null ) {
          val prevValue = target.getTagged
          if ( setPrevValue( prevValue ) ) {
            val expectValue = this.expectValue
            if ( expectValue == null ) {
              val expectValue = new TaggedValue( expect( this ) )
              if ( setExpectValue( expectValue ) ) {
                val updateValue = this.updateValue
                if ( updateValue == null ) {
                  val updateValue = new TaggedValue( update( this ) )
                  if ( setUpdateValue( updateValue ) ) {
                    /* Attempt to modify target */
                    val currentValue = target.getTagged
                    if ( currentValue == prevValue ) {
                      if ( currentValue.value == expectValue.value ) {
                        if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                          if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                            executeTR( sequence ) // unexpected
                        // else continue to the next op
                        } else executeTR( sequence ) // unexpected
                      } else { // fail
                        if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
                          executeTR( sequence ) // unexpected
                        // else continue to the next op
                      }
                    } else {
                      if ( currentValue == updateValue ) {
                        if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                          executeTR( sequence ) // unexpected
                        // else continue to the next op
                      } else executeTR( sequence ) // unexpected
                    }
                    /* Attempt to modify target */
                  } else executeTR( sequence ) // unexpected
                } else executeTR( sequence ) // unexpected
              } else executeTR( sequence ) // unexpected
            } else executeTR( sequence ) // unexpected
          } else executeTR( sequence ) // unexpected
        } else {
          val expectValue = this.expectValue
          if ( expectValue == null ) {
            val expectValue = new TaggedValue( expect( this ) )
            if ( setExpectValue( expectValue ) ) {
              val updateValue = this.updateValue
              if ( updateValue == null ) {
                val updateValue = new TaggedValue( update( this ) )
                if ( setUpdateValue( updateValue ) ) {
                  /* Attempt to modify target */
                  val currentValue = target.getTagged
                  if ( currentValue == prevValue ) {
                    if ( currentValue.value == expectValue.value ) {
                      if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                        if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                          executeTR( sequence ) // unexpected
                        // else continue to the next op
                      } else executeTR( sequence ) // unexpected
                    } else { // fail
                      if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
                        executeTR( sequence ) // unexpected
                      // else continue to the next op
                    }
                  } else {
                    if ( currentValue == updateValue ) {
                      if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                        executeTR( sequence ) // unexpected
                      // else continue to the next op
                    } else executeTR( sequence ) // unexpected
                  }
                  /* Attempt to modify target */
                } else executeTR( sequence ) // unexpected
              } else executeTR( sequence ) // unexpected
            } else executeTR( sequence ) // unexpected
          } else {
            val updateValue = this.updateValue
            if ( updateValue == null ) {
              val updateValue = new TaggedValue( update( this ) )
              if ( setUpdateValue( updateValue ) ) {
                /* Attempt to modify target */
                val currentValue = target.getTagged
                if ( currentValue == prevValue ) {
                  if ( currentValue.value == expectValue.value ) {
                    if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                      if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                        executeTR( sequence ) // unexpected
                      // else continue to the next op
                    } else executeTR( sequence ) // unexpected
                  } else { // fail
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
                      executeTR( sequence ) // unexpected
                    // else continue to the next op
                  }
                } else {
                  if ( currentValue == updateValue ) {
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                    // else continue to the next op
                  } else executeTR( sequence ) // unexpected
                }
                /* Attempt to modify target */
              } else executeTR( sequence ) // unexpected
            } else {
              /* Attempt to modify target */
              val currentValue = target.getTagged
              if ( currentValue == prevValue ) {
                if ( currentValue.value == expectValue.value ) {
                  if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                    // else continue to the next op
                  } else executeTR( sequence ) // unexpected
                } else { // fail
                  if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
                    executeTR( sequence ) // unexpected
                  // else continue to the next op
                }
              } else {
                if ( currentValue == updateValue ) {
                  if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                    executeTR( sequence ) // unexpected
                  // else continue to the next op
                } else executeTR( sequence ) // unexpected
              }
              /* Attempt to modify target */
            }
          }
        }
      }
      case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue to the next op
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  @inline
  @tailrec
  private def revertTR( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpSuccess => {
        val revertValue = this.revertValue
        if ( revertValue == null ) {
          val revertValue = new TaggedValue( prevValue.value )
          if ( setRevertValue( revertValue ) ) {
            revertTR( sequence ) // next step
          } else revertTR( sequence ) // unexpected
        } else {
          val updateValue = this.updateValue
          if ( updateValue == null ) {
            throw new RuntimeException( "What Happened" )
          } else {
            val currentValue = target.getTagged
            if ( currentValue == updateValue ) {
              if ( target.updateTagged( updateValue, revertValue ) ) { // success
                if ( ! updateOpStatus( CasnOpSuccess, CasnOpReverted ) )
                  revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            } else {
              val nextModifyOpForTarget = this.nextModifyOpForTarget
              if ( nextModifyOpForTarget != null ) {
                val nextModifyRevertValue = nextModifyOpForTarget.getRevertValue
                if ( nextModifyRevertValue != null ) {
                  if ( currentValue == nextModifyRevertValue ) {
                    if ( target.updateTagged( nextModifyRevertValue, revertValue ) ) { // success
                      if ( ! updateOpStatus( CasnOpSuccess, CasnOpReverted ) )
                        revertTR( sequence ) // unexpected
                    } else revertTR( sequence ) // unexpected
                  } else revertTR( sequence ) // unexpected
                } else revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            }
          }
        }
      }
      case CasnOpFailure | CasnOpReverted => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] final class GetOp[T]( _prevOp: CasnOp[_], target: CasnVar[T] ) extends CasnOp[T]( _prevOp ) {
  def getTarget = target
  override def execute( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpUndecided =>
        val prevValue = this.prevValue
        if ( prevValue == null )
          setPrevValue( target.getTagged )
        updateOpStatus( CasnOpUndecided, CasnOpSuccess )
      case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpSuccess => updateOpStatus( CasnOpSuccess, CasnOpReverted )
      case CasnOpReverted => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] final class ExpectTaggedOp[T]( _prevOp: CasnOp[_], target: CasnVar[T], expectTagged: TaggedValue[T] ) extends CasnOp[T]( _prevOp ) {
  def getTarget = target
  override def execute( sequence: CasnSequence[_] ) { executeTR( sequence ) }
  override def revert( sequence: CasnSequence[_] ) { revertTR( sequence ) }

  @inline
  override private[casn] def evaluateSingleExpect() = expectTagged == prevValue

  @inline
  @tailrec
  private def executeTR( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpUndecided => 
        val prevValue = this.prevValue
        if ( prevValue == null ) {
          setPrevValue( target.getTagged )
          executeTR( sequence ) // next step
        } else {
          if ( prevValue == expectTagged ) {
            if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
              executeTR( sequence ) // unexpected
          } else { // fail
            if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
              executeTR( sequence ) // unexpected
          } 
        }
      case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  @inline
  @tailrec
  private def revertTR( sequence: CasnSequence[_] ) {
    sequence.status match {
      case CasnSeqLocked => opStatus match {
        case CasnOpSuccess => {
          if ( ! updateOpStatus( CasnOpSuccess, CasnOpReverted ) )
            revertTR( sequence ) // unexpected
        }
        case CasnOpFailure | CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSeqAborted | CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] final class UpdateOp[T]( _prevOp: CasnOp[_], target: CasnVar[T], update: ( T ) => T ) extends CasnModifyOp[T]( _prevOp ) {
  def getTarget = target
  override def execute( sequence: CasnSequence[_] ) { executeTR( sequence ) }
  override def revert( sequence: CasnSequence[_] ) { revertTR( sequence ) }

  override private[casn] def evaluateSingleExpect() = true

  @inline
  @tailrec
  private def executeTR( sequence: CasnSequence[_] ) {
      opStatus match {
        case CasnOpUndecided =>
          val prevValue = this.prevValue
          if ( prevValue == null ) {
            val prevValue = target.getTagged
            if ( setPrevValue( prevValue ) ) {
              val updateValue = new TaggedValue( update( prevValue.value ) )
              if ( setUpdateValue( updateValue ) ) {
                /* Attempt to modify target */
                val currentValue = target.getTagged
                if ( currentValue == prevValue ) {
                  if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                  } else executeTR( sequence ) // unexpected
                } else {
                  if ( currentValue == updateValue ) {
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                  } else executeTR( sequence ) // unexpected
                }
                /* Attempt to modify target */
              } else executeTR( sequence ) // unexpected
            } else executeTR( sequence ) // unexpected
          } else {
            val updateValue = this.updateValue
            if ( updateValue == null ) {
              val updateValue = new TaggedValue( update( prevValue.value ) )
              if ( setUpdateValue( updateValue ) ) {
                /* Attempt to modify target */
                val currentValue = target.getTagged
                if ( currentValue == prevValue ) {
                  if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                  } else executeTR( sequence ) // unexpected
                } else {
                  if ( currentValue == updateValue ) {
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                  } else executeTR( sequence ) // unexpected
                }
                /* Attempt to modify target */
              } else executeTR( sequence ) // unexpected
            } else {
              /* Attempt to modify target */
              val currentValue = target.getTagged
              if ( currentValue == prevValue ) {
                if ( target.updateTagged( currentValue,  updateValue ) ) { // update target
                  if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                    executeTR( sequence ) // unexpected
                } else executeTR( sequence ) // unexpected
              } else {
                if ( currentValue == updateValue ) {
                  if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                    executeTR( sequence ) // unexpected
                } else executeTR( sequence ) // unexpected
              }
              /* Attempt to modify target */
            }
          }
        case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
  }

  @inline
  @tailrec
  private def revertTR( sequence: CasnSequence[_] ) {
    opStatus match {
      case CasnOpSuccess => {
        val revertValue = this.revertValue
        if ( revertValue == null ) {
          val revertValue = new TaggedValue( prevValue.value )
          if ( setRevertValue( revertValue ) ) {
            revertTR( sequence ) // next step
          } else revertTR( sequence ) // unexpected
        } else {
          val updateValue = this.updateValue
          if ( updateValue == null ) {
            throw new RuntimeException( "What Happened" )
          } else {
            val currentValue = target.getTagged
            if ( currentValue == updateValue ) {
              if ( target.updateTagged( updateValue, revertValue ) ) { // success
                if ( ! updateOpStatus( CasnOpSuccess, CasnOpReverted ) )
                  revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            } else {
              val nextModifyOpForTarget = this.nextModifyOpForTarget
              if ( nextModifyOpForTarget != null ) {
                val nextModifyRevertValue = nextModifyOpForTarget.getRevertValue
                if ( nextModifyRevertValue != null ) {
                  if ( currentValue == nextModifyRevertValue ) {
                    if ( target.updateTagged( nextModifyRevertValue, revertValue ) ) { // success
                      if ( ! updateOpStatus( CasnOpSuccess, CasnOpReverted ) )
                        revertTR( sequence ) // unexpected
                    } else revertTR( sequence ) // unexpected
                  } else revertTR( sequence ) // unexpected
                } else revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            }
          }
        }
      }
      case CasnOpFailure | CasnOpReverted => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

object CasnSequence {
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

  def cas[U]( target: CasnVar[U], expect: ( CasnOp[U] ) => U, update: ( CasnOp[U] ) => U ) = {
    new GenericOp( null, target, expect, update )
  }

  def setVal[U]( target: CasnVar[U], update: U ) = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )
  }

  def expectVal[U]( target: CasnVar[U], expect: U ) = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => op.prevValue.value )
  }

  def casVal[U]( target: CasnVar[U], expect: U, update: U ) = {
    new GenericOp( null, target, ( op: CasnOp[U] ) => expect, ( op: CasnOp[U] ) => update )
  }

  def casTagged[U]( target: CasnVar[U], expect: TaggedValue[U], update: ( CasnOp[U] ) => U ) =
              new ExpectTaggedOp( null, target, expect )
              .cas( target, ( op: CasnOp[U] ) => op.prevValue.value, update )

  def casTaggedVal[U]( target: CasnVar[U], expect: TaggedValue[U], update: U ) =
      new ExpectTaggedOp( null, target, expect )
      .cas( target, ( op: CasnOp[U] ) => op.prevValue.value, ( op: CasnOp[U] ) => update )

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

  private[casn] val identityCounter = new AtomicLong
  private[casn] val blockingTestCounter = new AtomicLong
  def getIdentityCount = identityCounter.get()
  def getBlockingTestCount = blockingTestCounter.get()
}
final class CasnSequence[T]( lastOp: CasnOp[T] ) {
  import CasnSequence._
  val identity = identityCounter.incrementAndGet()
  val firstOp: CasnOp[_] = getFirstOp( lastOp )
  val readOnly: Boolean = isReadOnly( lastOp )
  val isSingleOp = firstOp == lastOp
  if ( ! isSingleOp )
    initializeOps( firstOp )

  @volatile var status: CasnSeqStatus = CasnSeqUndecided
  @volatile var preLockOp: CasnOp[_] = null
  @volatile var lockOp: CasnOp[_] = null
  @volatile var updateOp: CasnOp[_] = null
  @volatile var revertOp: CasnOp[_] = null
  @volatile var releaseOp: CasnOp[_] = null

  @inline
  def getLastOp = lastOp
  
  
  @inline
  def isConstructing = status == CasnSeqConstructing

  @inline
  def isUndecided = status == CasnSeqUndecided || status == CasnSeqPreLocked || status == CasnSeqLocked

  @inline
  def isPreLocked = status == CasnSeqPreLocked

  @inline
  def isLocked = status == CasnSeqLocked

  @inline
  def isSuccess = status == CasnSeqSuccess || status == CasnSeqSuccessReleased

  @inline
  def isFailure = status == CasnSeqFailure || status == CasnSeqFailureReleased

  @inline
  private def setNextPreLockOp( expect: CasnOp[_], update: CasnOp[_] ): Boolean =
    Unsafe.compareAndSwapObject( this, sequencePreLockOpIndex, expect, update )

  @inline
  private def setNextLockOp( expect: CasnOp[_], update: CasnOp[_] ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceLockOpIndex, expect, update )

  @inline
  private def setNextUpdateOp( expect: CasnOp[_], update: CasnOp[_] ): Boolean =
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
  private def hasPriorityOver( other: CasnSequence[_] ): Boolean = identity < other.identity

  @inline
  @tailrec
  private def initializeOps( op: CasnOp[_] ) {
    if ( op != null ) {
      op.initialize()
      initializeOps( op.nextOp )
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
  private def process( sequence: CasnSequence[_] ): Boolean = {
    if ( sequence.isSingleOp ) processTR( Nil, sequence )
    else {
      val firstOp = sequence.firstOp
      sequence.preLockOp = firstOp
      sequence.lockOp = firstOp
      sequence.updateOp = firstOp
      sequence.releaseOp = lastOp
      processTR( Nil, sequence )
    }
  }

  @inline
  private def updatePreLock( sequence: CasnSequence[_], op: CasnOp[_], target: CasnVar[_], currentLockValue: CasnLockValue, currentLock: CasnLock ) {
    if ( sequence.status == CasnSeqUndecided )
      if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock, true ) ) ) )
        preLockingNextStep( sequence, op )
  }

  @inline
  private def preLockingNextStep( sequence: CasnSequence[_], op: CasnOp[_] ) {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextPreLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqUndecided, CasnSeqPreLocked )
  }

  @inline
  private def updateLock( sequence: CasnSequence[_], op: CasnOp[_], target: CasnVar[_], currentLockValue: CasnLockValue, currentLock: CasnLock ) {
    if ( sequence.status == CasnSeqPreLocked )
      if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock.next, false ) ) ) )
        lockingNextStep( sequence, op )
  }

  @inline
  private def lockingNextStep( sequence: CasnSequence[_], op: CasnOp[_] ) {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqPreLocked, CasnSeqLocked )
//    else sequence.updateStatus( CasnSeqPreLocked, CasnSeqSuccess )
  }

  @inline
  private def updatingNextStep( sequence: CasnSequence[_], op: CasnOp[_] ) {
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

  @inline
  @tailrec
  private def unlink( sequence: CasnSequence[_], op: CasnOp[_] ): Boolean = {
    if ( op == null )
      sequence.status == CasnSeqSuccessReleased
    else {
      op.unlink()
      unlink( sequence, op.prevOp )
    }
  }

  @tailrec
  private def processTR( sequences: List[CasnSequence[_]], sequence: CasnSequence[_] ): Boolean = {
    val sequenceStatus = sequence.status
    sequenceStatus match {
      case CasnSeqUndecided => { // PreLocking
        if ( sequence.isSingleOp ) {
          if ( sequence.readOnly ) { // Single ReadOnly Op
            val op = sequence.getLastOp
            val target = op.getTarget
            val currentLockValue = target.lockValue
            val currentLock = currentLockValue.lock
            if ( currentLock == null ) {
              val prevValue = op.prevValue
              if ( prevValue == null ) {
                // not currently locked or pre locked, attempt lock because we are a single sequence
                val newLock = CasnLock( target, sequence, currentLock, false )
                val newLockValue = new CasnLockValue( newLock )
                if ( target.updateLockValue( currentLockValue, newLockValue ) ) {
                  if ( op.executeSingleRead() ) {
                    val postLockValue = new CasnLockValue( currentLock )
                    if ( target.updateLockValue( newLockValue, postLockValue ) ) {
                      sequences match {
                        case head :: tail => processTR( tail, head )
                        case Nil => op.evaluateSingleExpect()
                      }
                    } else processTR( sequences, sequence ) // try again
                  } else processTR( sequences, sequence ) // try again
                } else processTR( sequences, sequence ) // try again
              } else sequences match { // We already succeeded
                case head :: tail => processTR( tail, head )
                case Nil => op.evaluateSingleExpect()
              }
            } else {
              val currentLockSequence = currentLock.sequence
              if ( sequence == currentLockSequence ) { // We have it locked
                val prevValue = op.prevValue
                if ( prevValue == null ) {
                  if ( op.executeSingleRead() ) {
                    val postLockValue = new CasnLockValue( currentLock.next )
                    if ( target.updateLockValue( currentLockValue, postLockValue ) ) {
                      sequences match {
                        case head :: tail => processTR( tail, head )
                        case Nil => op.evaluateSingleExpect()
                      }
                    } else processTR( sequences, sequence ) // try again
                  } else processTR( sequences, sequence ) // try again
                } else {
                  val postLockValue = new CasnLockValue( currentLock.next )
                  if ( target.updateLockValue( currentLockValue, postLockValue ) ) {
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
                  case Nil => op.evaluateSingleExpect()
                }
              } 
            }
          } else { // Single Modify Op
            val op = sequence.getLastOp
            val target = op.getTarget
            val currentLockValue = target.getLockValue
            val currentLock = currentLockValue.lock
            op.opStatus match {
              case CasnOpUndecided =>
                if ( currentLock == null ) {
                  // not currently locked or pre locked, attempt lock because we are a single sequence
                  val newLock = CasnLock( target, sequence, currentLock, false )
                  val newLockValue = new CasnLockValue( newLock )
                  if ( target.updateLockValue( currentLockValue, newLockValue ) ) {
                    op.execute( this )
                    val postLockValue = new CasnLockValue( newLock.next )
                    if ( target.updateLockValue( newLockValue, postLockValue ) ) {
                      sequences match { // Completed
                        case head :: tail => processTR( tail, head )
                        case Nil => op.opStatus == CasnOpSuccess
                      }                      
                    } else processTR( sequences, sequence ) // try again                    
                  } else processTR( sequences, sequence ) // try again
                } else { 
                  val currentLockSequence = currentLock.sequence
                  if ( sequence == currentLockSequence ) { // We have the lock already
                    op.execute( this )
                    val postLockValue = new CasnLockValue( currentLock.next )
                    if ( target.updateLockValue( currentLockValue, postLockValue ) ) {
                      sequences match { // Completed
                        case head :: tail => processTR( tail, head )
                        case Nil => op.opStatus == CasnOpSuccess
                      }                      
                    } else processTR( sequences, sequence ) // try again                    
                  } else {
                    val newSequences = sequence :: sequences
                    processTR( newSequences, currentLockSequence ) // help out
                  }
                }
              case CasnOpSuccess | CasnOpFailure =>
                if ( currentLock == null ) {
                  sequences match { // Completed
                    case head :: tail => processTR( tail, head )
                    case Nil => op.opStatus == CasnOpSuccess
                  }
                } else {
                  val currentLockSequence = currentLock.sequence
                  if ( sequence == currentLockSequence ) { // We still have lock
                    val postLockValue = new CasnLockValue( currentLock.next )
                    if ( target.updateLockValue( currentLockValue, postLockValue ) ) {
                      sequences match { // Completed
                        case head :: tail => processTR( tail, head )
                        case Nil => op.opStatus == CasnOpSuccess
                      }                      
                    } else processTR( sequences, sequence ) // try again                    
                  } else {
                    sequences match { // Completed
                      case head :: tail => processTR( tail, head )
                      case Nil => op.opStatus == CasnOpSuccess
                    }
                  }                  
                }
              case CasnOpReverted => throw new RuntimeException( "What Happened" )
            }
          }
        } else { // Not a Single Op
          val op = sequence.preLockOp
          if ( op.targetPreviouslyLocked ) {
            preLockingNextStep( sequence, op )
            processTR( sequences, sequence )
          } else {
            val target = op.getTarget
            val currentLockValue = target.getLockValue
            val currentLock = currentLockValue.lock
            if ( currentLock == null ) {
              // not currently locked or pre locked, attempt pre lock
              updatePreLock( sequence, op, target, currentLockValue, currentLock )
              processTR( sequences, sequence )
            } else { 
              val currentLockSequence = currentLock.sequence
              if ( sequence == currentLockSequence || onLockChain( currentLock.next, sequence ) ) {
                // already ( prelocked or locked ) move on to the next step
                preLockingNextStep( sequence, op )
                processTR( sequences, sequence )
              } else if ( currentLock.isPreLock ) {
                // currently pre locked
                if ( sequence.hasPriorityOver( currentLockSequence ) ) {
                  // we have priority, grab the pre lock
                  updatePreLock( sequence, op, target, currentLockValue, currentLock )
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
                if ( op == sequence.firstOp ) {
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
                      updatePreLock( sequence, op, target, currentLockValue, currentLock )
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
      }
      case CasnSeqPreLocked => { // Locking
        val op = sequence.lockOp
        if ( op.targetPreviouslyLocked ) {
          lockingNextStep( sequence, op )
          processTR( sequences, sequence )
        } else {
          val target = op.getTarget
          val currentLockValue = target.getLockValue
          val currentLock = currentLockValue.lock
          if ( currentLock == null )
            processTR( sequences, sequence ) // this can happen, re-run step
          else {
            val currentLockSequence = currentLock.sequence
            if ( sequence == currentLockSequence ) { // if we have an immediate lock on it
              if ( currentLock.isPreLock ) { // and we have a pre lock then attempt to replace pre lock with lock
                updateLock( sequence, op, target, currentLockValue, currentLock )
              } else lockingNextStep( sequence, op ) // otherwise already locked, move on to the next one
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
      }
      case CasnSeqLocked => { // Updating
        val op = sequence.updateOp
        val currentLockValue = op.getTarget.getLockValue
        val currentLock = currentLockValue.lock
        op.opStatus match {
          case CasnOpUndecided => {
            if ( currentLock == null || currentLock.sequence != sequence )
              throw new RuntimeException( "This should not happen" )
            op.execute( sequence ) // this should set the op status, if not then there was an unexpected state
            processTR( sequences, sequence ) // in any case keep going
          } 
          case CasnOpSuccess => {
            updatingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case CasnOpFailure => {
            if ( sequence.readOnly ) {
              sequence.updateStatus( CasnSeqLocked, CasnSeqFailure )
              processTR( sequences, sequence )
            } else {
              val prevOp = op.prevOp
              if ( prevOp != null ) {
                setNextRevertOp( null, prevOp )
                sequence.updateStatus( CasnSeqLocked, CasnSeqAborted )
                processTR( sequences, sequence )
              } else {
                sequence.updateStatus( CasnSeqLocked, CasnSeqFailure )
                processTR( sequences, sequence )
              }
            }
          }
          case CasnOpReverted => {
            // unexpected, already in reverted pipe, try again
            processTR( sequences, sequence )
          } 
        }
      }
      case CasnSeqAborted => { // Reverting
        val op = sequence.revertOp
        op.opStatus match {
          // the CasnSeqUndecided and CasnSeqFailure case shouldn't occur on op after this method is called
          case CasnOpSuccess => {
            op.revert( sequence ) // this should set the op status, if not then there was an unexpected state
            if ( op.opStatus == CasnOpReverted )
              revertingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case CasnOpReverted => {
            revertingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case _ => throw new RuntimeException( "What Happened!" )
        }
      }
      case CasnSeqSuccess | CasnSeqFailure => { // Releasing
        val op = sequence.releaseOp
        if ( ! op.targetPreviouslyLocked ) {
          val target = op.getTarget
          val currentLockValue = target.getLockValue
          val currentLock = currentLockValue.lock
          if ( currentLock != null && currentLock.sequence == sequence ) {
            if ( target.updateLockValue( currentLockValue, new CasnLockValue( currentLock.next ) ) )
              releasingNextStep( sequence, sequenceStatus, op )
          } else if ( onLockChain( currentLock, sequence ) )
            throw new RuntimeException( "This shouldn't happen" )
          else releasingNextStep( sequence, sequenceStatus, op )
        } else
          releasingNextStep( sequence, sequenceStatus, op )
        processTR( sequences, sequence )
      }
      case CasnSeqSuccessReleased | CasnSeqFailureReleased => sequences match {
        case head :: tail => processTR( tail, head )
        case Nil =>
          if ( sequence.isSingleOp ) sequence.status == CasnSeqSuccessReleased
          else unlink( sequence, sequence.getLastOp )
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
    val firstOp = targetSequence.firstOp
    if ( firstOp == null ) throw new RuntimeException( "What Happened" )
    else areWeBlockingTargetSequenceLock( Nil, sequence, targetSequence, firstOp, firstOp.getTarget.getLockValue.lock )
  }

  @inline
  @tailrec
  private def areWeBlockingTargetSequenceLock( sequences: List[CasnSequence[_]], sequence: CasnSequence[_], targetSequence: CasnSequence[_], op: CasnOp[_], lock: CasnLock ): Boolean = {
    if ( lock == null ) false // something has changed, break out with false 
    else {
      val lockSequence = lock.sequence
      if ( lockSequence == targetSequence ) {
        val nextOp = op.nextOp
        if ( nextOp == null ) sequences match {
          case Nil => false
          case head :: tail =>
            val firstOp = targetSequence.firstOp
            if ( firstOp == null ) throw new RuntimeException( "What Happened" )
            areWeBlockingTargetSequenceLock( tail, sequence, head, firstOp, firstOp.getTarget.getLockValue.lock )
        }
        else areWeBlockingTargetSequenceLock( sequences, sequence, targetSequence, nextOp, nextOp.getTarget.getLockValue.lock )
      }
      else if ( lockSequence == sequence ) true
      else if ( sequences.contains( lockSequence ) )
        areWeBlockingTargetSequenceLock( sequences, sequence, targetSequence, op, lock.next )
      else areWeBlockingTargetSequenceLock( lockSequence :: sequences, sequence, targetSequence, op, lock.next )
    }
  }
}
