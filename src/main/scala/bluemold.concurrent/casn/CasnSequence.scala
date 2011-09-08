package bluemold.concurrent.casn

import org.bluemold.unsafe.Unsafe
import annotation.tailrec
import Unsafe._

object CasnVar {
  val valueIndex = objectDeclaredFieldOffset( classOf[CasnVar], "value" )
  val lockValueIndex = objectDeclaredFieldOffset( classOf[CasnVar], "lockValue" )
  def create(): CasnVar = create( null )
  def create( value: Any ): CasnVar = new CasnVar().initialize( new TaggedValue( value ) )
}
class CasnVar {
  import CasnVar._
  @volatile var value: TaggedValue = new TaggedValue( null )
  @volatile var lockValue: CasnLockValue = new CasnLockValue( null )

  private[casn] def initialize( value: TaggedValue ): CasnVar = { this.value = value; this }

  private[casn] def getLockValue = lockValue

  private[casn] def updateTagged( expect: TaggedValue, update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, valueIndex, expect, update )

  private[casn] def updateLockValue( expect: CasnLockValue, update: CasnLockValue ): Boolean =
    Unsafe.compareAndSwapObject( this, lockValueIndex, expect, update )
  
  def getValue: Any = value.value
  def getTagged: TaggedValue = value
  def safeGet(): Any = new CasnSequence().executeAndGet( this )
  def safeGetTagged(): TaggedValue = new CasnSequence().executeAndGetTagged( this )
  def set( value: Any ) { new CasnSequence().setVal( this, value ).execute() }
  def cas( expect: Any, update: Any ): Boolean = new CasnSequence().casVal( this, expect, update ).execute()
}

abstract class CasnFun {
  def get( op: CasnOp ): Any
}

private[concurrent] abstract class CasnSafeFun extends CasnFun {
  def get( op: CasnOp ): Any
}

case class Value( value: Any ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = value
}

case class Prior( index: Int ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = op.prior( index )
}

case class PriorEq( index: Int, value: Any ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = op.prior( index ) != value
}

case class PriorNotEq( index: Int, value: Any ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = op.prior( index ) != value
}

case class PriorEqPrior( index: Int, otherIndex: Int ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = op.prior( index ) == op.prior( otherIndex )
}

case class PriorNotEqPrior( index: Int, otherIndex: Int ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = op.prior( index ) != op.prior( otherIndex )
}

case class Cond( cond: CasnFun, then: CasnFun, otherwise: CasnFun ) extends CasnFun {
  def get( op: CasnOp ): Any = {
    cond.get( op ) match {
      case true => then.get( op )
      case _ => otherwise.get( op )
    }
  }
}

case class SafeCond( cond: CasnSafeFun, then: CasnSafeFun, otherwise: CasnSafeFun ) extends CasnSafeFun {
  def get( op: CasnOp ): Any = {
    cond.get( op ) match {
      case true => then.get( op )
      case _ => otherwise.get( op )
    }
  }
}

class TaggedValue( _value: Any ) {
  val value = _value
}
private[casn] class CasnLockValue( _lock: CasnLock ) {
  val lock = _lock
}
private[casn] case class CasnLock( target: CasnVar, sequence: CasnSequence, next: CasnLock, isPreLock: Boolean )

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
  val opStatusIndex = objectDeclaredFieldOffset( classOf[CasnOp], "opStatus" ) 
  val prevValueIndex = objectDeclaredFieldOffset( classOf[CasnOp], "prevValue" ) 
}
abstract class CasnOp {
  import CasnOp._
  @volatile var opStatus: CasnOpStatus = CasnOpUndecided
  @volatile var nextOp: CasnOp = null
  @volatile var prevOp: CasnOp = null
  @volatile var prevValue: TaggedValue = null
  final def getPrevValue: TaggedValue = prevValue

  private[casn] def updateOpStatus( expect: CasnOpStatus, update: CasnOpStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, opStatusIndex, expect, update )

  private[casn] def setPrevValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, prevValueIndex, null, update )

  @tailrec
  final def fail() {
    opStatus match {
      case CasnOpUndecided => if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) ) fail()
      case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
    }
  }

  final def prior(index: Int): Any = prior( this, index )

  @tailrec
  final def prior( op: CasnOp, index: Int ): Any = {
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

  def execute( sequence: CasnSequence )
  def revert( sequence: CasnSequence )
  def copy(): CasnOp
  def copy( target: CasnVar ): CasnOp
  def getTarget: CasnVar
  def getUpdateValue: TaggedValue
}

private[casn] object GenericOp {
  val updateValueIndex = objectDeclaredFieldOffset( classOf[GenericOp], "updateValue" ) 
  val expectValueIndex = objectDeclaredFieldOffset( classOf[GenericOp], "expectValue" ) 
}
private[casn] case class GenericOp( target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any ) extends CasnOp {
  import GenericOp._
  @volatile var updateValue: TaggedValue = null
  @volatile var expectValue: TaggedValue = null

  final def copy(): CasnOp = GenericOp( target, expect, update )
  final def copy( target: CasnVar ): CasnOp = GenericOp( target, expect, update )
  final def getTarget = target
  final def getUpdateValue = updateValue
  
  private[casn] def setExpectValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, expectValueIndex, null, update )

  private[casn] def setUpdateValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, updateValueIndex, null, update )

  override def execute( sequence: CasnSequence ) {
    executeTR( sequence )
  }

  @tailrec
  private final def executeTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnSeqLocked => opStatus match {
        case CasnOpUndecided => {
          val prevValue = this.prevValue
          if ( prevValue == null ) {
            setPrevValue( target.getTagged )
            executeTR( sequence ) // next step
          } else {
            val expectValue = this.expectValue
            if ( expectValue == null ) {
              setExpectValue( new TaggedValue( expect( this ) ) )
              executeTR( sequence ) // next step
            } else {
              val updateValue = this.updateValue
              if ( updateValue == null ) {
                setUpdateValue( new TaggedValue( update( this ) ) )
                executeTR( sequence ) // next step
              } else {
                val currentValue = target.getTagged
                if ( currentValue == prevValue ) {
                  if ( currentValue.value == expectValue.value ) {
                    target.updateTagged( currentValue,  updateValue ) // update target
                    executeTR( sequence ) // next step
                  } else { // fail
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpFailure ) )
                      executeTR( sequence ) // unexpected
                  }
                } else {
                  if ( currentValue == updateValue ) {
                    if ( ! updateOpStatus( CasnOpUndecided, CasnOpSuccess ) )
                      executeTR( sequence ) // unexpected
                    else executeTR( sequence ) // tail recursion test
                  } else executeTR( sequence ) // unexpected
                }
              }
            }
          }
        }
        case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSeqAborted | CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence ) {
    revertTR( sequence )
  }

  @tailrec
  private final def revertTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnSeqLocked | CasnSeqAborted => opStatus match {
        case CasnOpSuccess => {
          val preRevertValue = this.updateValue
          if ( preRevertValue == null ) {
            throw new RuntimeException( "What Happened" )
          } else {
            val currentValue = target.getTagged
            if ( currentValue == preRevertValue ) {
              if ( target.updateTagged( preRevertValue, new TaggedValue( prevValue.value ) ) ) { // success
                if ( updateOpStatus( CasnOpSuccess, CasnOpReverted ) ) return
                else revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            } else revertTR( sequence ) // unexpected
          }
        }
        case CasnOpFailure | CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] case class GetOp( target: CasnVar ) extends CasnOp {
  final def copy() = GetOp( target )
  final def copy(target: CasnVar) = GetOp( target )
  final def getTarget = target
  final def getUpdateValue = getPrevValue
  override def execute( sequence: CasnSequence ) {
    sequence.status match {
      case CasnSeqLocked => opStatus match {
        case CasnOpUndecided =>
          val prevValue = this.prevValue
          if ( prevValue == null )
            setPrevValue( target.getTagged )
          updateOpStatus( CasnOpUndecided, CasnOpSuccess )
        case CasnOpSuccess | CasnOpFailure | CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSeqAborted | CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence ) {
    sequence.status match {
      case CasnSeqLocked => opStatus match {
        case CasnOpSuccess => updateOpStatus( CasnOpSuccess, CasnOpReverted )
        case CasnOpReverted => // continue
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSeqAborted | CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] case class ExpectTaggedOp( target: CasnVar, expectTagged: TaggedValue ) extends  CasnOp {
  final def copy() = ExpectTaggedOp( target, expectTagged )
  final def copy(target: CasnVar) = ExpectTaggedOp( target, expectTagged )
  final def getTarget = target
  final def getUpdateValue = getPrevValue
  override def execute( sequence: CasnSequence ) {
    executeTR( sequence )
  }
  @tailrec
  private final def executeTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnSeqLocked => opStatus match {
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
      case CasnSeqAborted | CasnSeqSuccess | CasnSeqFailure | CasnSeqSuccessReleased | CasnSeqFailureReleased => // continue
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence ) {
    revertTR( sequence )
  }
  @tailrec
  private final def revertTR( sequence: CasnSequence ) {
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
private[casn] object CasnSequence {
  val sequenceStatusIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "status" ) 
  val sequencePreLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "preLockOp" ) 
  val sequenceLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "lockOp" ) 
  val sequenceUpdateOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "updateOp" ) 
  val sequenceRevertOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "revertOp" ) 
  val sequenceReleaseOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "releaseOp" ) 
}
class CasnSequence {
  import CasnSequence._
  @volatile var firstOp: CasnOp = null
  @volatile var lastOp: CasnOp = null
  @volatile var status: CasnSeqStatus = CasnSeqConstructing
  @volatile var preLockOp: CasnOp = null
  @volatile var lockOp: CasnOp = null
  @volatile var updateOp: CasnOp = null
  @volatile var revertOp: CasnOp = null
  @volatile var releaseOp: CasnOp = null

  def isConstructing = status == CasnSeqConstructing
  def isUndecided = status == CasnSeqUndecided || status == CasnSeqPreLocked || status == CasnSeqLocked
  def isPreLocked = status == CasnSeqPreLocked
  def isLocked = status == CasnSeqLocked
  def isSuccess = status == CasnSeqSuccess || status == CasnSeqSuccessReleased
  def isFailure = status == CasnSeqFailure || status == CasnSeqFailureReleased

  private def setNextPreLockOp( expect: CasnOp, update: CasnOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequencePreLockOpIndex, expect, update )

  private def setNextLockOp( expect: CasnOp, update: CasnOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceLockOpIndex, expect, update )

  private def setNextUpdateOp( expect: CasnOp, update: CasnOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceUpdateOpIndex, expect, update )

  private def setNextRevertOp( expect: CasnOp, update: CasnOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceRevertOpIndex, expect, update )

  private def setNextReleaseOp( expect: CasnOp, update: CasnOp ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceReleaseOpIndex, expect, update )

  private def updateStatus( expect: CasnSeqStatus, update: CasnSeqStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, sequenceStatusIndex, expect, update )

  private def hasPriorityOver( other: CasnSequence ): Boolean = {
    if ( this == other ) false
    else Unsafe.naturalReferenceCompare( this, other ) == -1
  }
  
  // Only meant to be called by a single thread prior to
  // the sequence being added to the concurrent environment
  // via the execute method by that same thread
  private def addOp0( op: CasnOp ) {
    if ( firstOp == null ) {
      firstOp = op
      lastOp = op
    } else {
      op.prevOp = lastOp
      lastOp.nextOp = op
      lastOp = op
    }
  }

  def get( target: CasnVar ) = {
    if ( isConstructing )
      addOp0( GetOp( target ) )
    else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def set( target: CasnVar, update: CasnFun ) = {
    addOp( target, ( op: CasnOp ) => op.prior( 0 ), ( op: CasnOp ) => update.get( op ) )
  }

  def set( target: CasnVar, update: ( CasnOp ) => Any ) = {
    addOp( target, ( op: CasnOp ) => op.prior( 0 ), update )
  }

  def expect( target: CasnVar, expect: ( CasnOp ) => Any ) = {
    addOp( target, expect, ( op: CasnOp ) => op.prior( 0 ) )
  }

  def expect( target: CasnVar, expect: CasnFun ) = {
    addOp( target, ( op: CasnOp ) => expect.get( op ), ( op: CasnOp ) => op.prior( 0 ) )
  }

  def cas( target: CasnVar, expect: CasnFun, update: CasnFun ) = {
    addOp( target, ( op: CasnOp ) => expect.get( op ), ( op: CasnOp ) => update.get( op ) )
  }

  def cas( target: CasnVar, expect: CasnFun, update: ( CasnOp ) => Any ) = {
    addOp( target, ( op: CasnOp ) => expect.get( op ), update )
  }

  def cas( target: CasnVar, expect: ( CasnOp ) => Any, update: CasnFun ) = {
    addOp( target, expect, ( op: CasnOp ) => update.get( op ) )
  }

  def cas( target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any ) = {
    addOp( target, expect, update )
  }

  def setVal( target: CasnVar, update: Any ) = {
    addOp( target, ( op: CasnOp ) => op.prior( 0 ), _ => update )
  }

  def expectVal( target: CasnVar, expect: Any ) = {
    addOp( target, _ => expect, ( op: CasnOp ) => op.prior( 0 ) )
  }

  def casVal( target: CasnVar, expect: Any, update: Any ) = {
    addOp( target, _ => expect, _ => update )
  }

  def casTagged( target: CasnVar, expect: TaggedValue, update: ( CasnOp ) => Any ) = {
    if ( isConstructing ) {
      addOp0( ExpectTaggedOp( target, expect ) )
      addOp0( GenericOp( target, ( op: CasnOp ) => op.prior( 0 ), update ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def casTagged( target: CasnVar, expect: TaggedValue, update: CasnFun ) = {
    if ( isConstructing ) {
      addOp0( ExpectTaggedOp( target, expect ) )
      addOp0( GenericOp( target, ( op: CasnOp ) => op.prior( 0 ), ( op: CasnOp ) => update.get( op ) ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def casTaggedVal( target: CasnVar, expect: TaggedValue, update: Any ) = {
    if ( isConstructing ) {
      addOp0( ExpectTaggedOp( target, expect ) )
      addOp0( GenericOp( target, ( op: CasnOp ) => op.prior( 0 ), _ => update ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def expectTagged( target: CasnVar, expect: TaggedValue ) = {
    if ( isConstructing )
      addOp0( ExpectTaggedOp( target, expect ) )
    else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  private def addOp( target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any ) = {
    if ( isConstructing )
      addOp0( GenericOp( target, expect, update ) )
    else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def copy(): CasnSequence = {
    copy0( new CasnSequence(), firstOp )
  }

  def copy( targets: List[CasnVar] ): CasnSequence = {
    copy0( new CasnSequence(), firstOp, targets )
  }

  @tailrec
  protected final def copy0( sequence: CasnSequence, op: CasnOp ): CasnSequence = {
    if ( op != null ) {
      sequence.addOp0( op.copy() )
      copy0( sequence, op.nextOp )
    } else sequence
  }

  @tailrec
  protected final def copy0( sequence: CasnSequence, op: CasnOp, targets: List[CasnVar] ): CasnSequence = {
    if ( op != null ) {
      targets match {
        case target :: tail => {
          sequence.addOp0( op.copy( target ) )
          copy0( sequence, op.nextOp, tail )
        }
        case _ => throw new IllegalArgumentException( "Not enough targets for sequence")
      }
    } else sequence
  }

  def execute(): Boolean = {
    if ( updateStatus( CasnSeqConstructing, CasnSeqUndecided ) ) {
      if ( firstOp != null )
        process( this )
      else true
    } else false
  }

  def getLast: Any = {
    if ( isConstructing )
      throw new IllegalStateException( "sequence not executed yet" )
    if ( status != CasnSeqSuccess )
      throw new IllegalStateException( "sequence did not complete successfully" )
    if ( firstOp == null || lastOp == null )
      throw new IllegalStateException( "no operations defined!" )
    get0( lastOp, 0 )
  }

  def get( index: Int ): Any = {
    if ( index < 0 )
      throw new IllegalArgumentException( "index out of bounds" )
    if ( isConstructing )
      throw new IllegalStateException( "sequence not executed yet" )
    if ( status != CasnSeqSuccess )
      throw new IllegalStateException( "sequence did not complete successfully" )
    if ( firstOp == null || lastOp == null )
      throw new IllegalStateException( "no operations defined!" )
    get0( firstOp, index )
  }
    
  @tailrec
  private final def get0( op: CasnOp, index: Int ): Any = {
    if ( op != null ) {
      if ( index < 0 )
        throw new RuntimeException( "What Happened!" )
      else if ( index == 0 ) {
        val prevValue = op.getPrevValue
        if ( prevValue == null )
          throw new RuntimeException( "What Happened!" )
        prevValue.value
      } else get0( op.nextOp, index - 1 )
    } else throw new IllegalArgumentException( "index out of bounds" )
  }

  def executeAndGet( target: CasnVar ): Any = {
    if ( isConstructing ) {
      addOp0( GetOp( target ) )
      if ( updateStatus( CasnSeqConstructing, CasnSeqUndecided ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val prevValue = lastOp.getPrevValue
            if ( prevValue != null ) prevValue.value
            else throw new IllegalStateException( "sequence did not complete successfully" )
          } else throw new IllegalStateException( "sequence did not complete successfully" )
        } else throw new IllegalStateException( "no operations defined!" )
      } else throw new IllegalStateException( "sequence already constructed" )
    } else throw new IllegalStateException( "sequence already constructed" );
  }

  def executeAndGet(): Any = {
    if ( isConstructing ) {
      if ( updateStatus( CasnSeqConstructing, CasnSeqUndecided ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val lastValue = lastOp.getUpdateValue
            if ( lastValue != null ) lastValue.value
            else throw new IllegalStateException( "sequence did not complete successfully" )
          } else throw new IllegalStateException( "sequence did not complete successfully" )
        } else throw new IllegalStateException( "no operations defined!" )
      } else throw new IllegalStateException( "sequence already constructed" )
    } else throw new IllegalStateException( "sequence already constructed" );
  }

  def executeOption(): Option[Any] = {
    if ( isConstructing ) {
      if ( updateStatus( CasnSeqConstructing, CasnSeqUndecided ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val lastValue = lastOp.getUpdateValue
            if ( lastValue != null ) Some( lastValue.value )
            else None
          } else None
        } else throw new IllegalStateException( "no operations defined!" )
      } else throw new IllegalStateException( "sequence already constructed" )
    } else throw new IllegalStateException( "sequence already constructed" );
  }

  def executeAndGetTagged( target: CasnVar ): TaggedValue = {
    if ( isConstructing ) {
      addOp0( GetOp( target ) )
      if ( updateStatus( CasnSeqConstructing, CasnSeqUndecided ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val prevValue = lastOp.getPrevValue
            if ( prevValue != null ) prevValue
            else throw new IllegalStateException( "sequence did not complete successfully" )
          } else throw new IllegalStateException( "sequence did not complete successfully" )
        } else throw new IllegalStateException( "no operations defined!" )
      } else throw new IllegalStateException( "sequence already constructed" )
    } else throw new IllegalStateException( "sequence already constructed" );
  }

  private final def process( sequence: CasnSequence ): Boolean = {
    val firstOp = sequence.firstOp
    sequence.preLockOp = firstOp
    sequence.lockOp = firstOp
    sequence.updateOp = firstOp
    sequence.releaseOp = firstOp
    processTR( Nil, sequence )
  }

  private final def updatePreLock( sequence: CasnSequence, op: CasnOp, target: CasnVar, currentLockValue: CasnLockValue, currentLock: CasnLock ) = {
    if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock, true ) ) ) ) {
      preLockingNextStep( sequence, op )
    }
  }

  private final def preLockingNextStep( sequence: CasnSequence, op: CasnOp ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextPreLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqUndecided, CasnSeqPreLocked )
  }

  private final def updateLock( sequence: CasnSequence, op: CasnOp, target: CasnVar, currentLockValue: CasnLockValue, currentLock: CasnLock ) = {
    if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock.next, false ) ) ) ) {
      lockingNextStep( sequence, op )
    }
  }

  private final def lockingNextStep( sequence: CasnSequence, op: CasnOp ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextLockOp( op, nextOp )
    else sequence.updateStatus( CasnSeqPreLocked, CasnSeqLocked )
  }

  private final def updatingNextStep( sequence: CasnSequence, op: CasnOp ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextUpdateOp( op, nextOp )
    else sequence.updateStatus( CasnSeqLocked, CasnSeqSuccess )
  }

  private final def revertingNextStep( sequence: CasnSequence, op: CasnOp ) = {
    val prevOp = op.prevOp
    if ( prevOp != null ) setNextRevertOp( op, prevOp )
    else sequence.updateStatus( CasnSeqAborted, CasnSeqFailure )
  }

  private final def releasingNextStep( sequence: CasnSequence, sequenceStatus: CasnSeqStatus, op: CasnOp ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextReleaseOp( op, nextOp )
    else if ( sequenceStatus == CasnSeqSuccess ) sequence.updateStatus( CasnSeqSuccess, CasnSeqSuccessReleased )
    else sequence.updateStatus( CasnSeqFailure, CasnSeqFailureReleased )
  }

  @tailrec
  private final def unlink( sequence: CasnSequence, op: CasnOp ): Boolean = {
    if ( op == null )
      sequence.status == CasnSeqSuccessReleased
    else {
      op.prevOp = null
      unlink( sequence, op.nextOp )
    }
  }

  @tailrec
  private final def processTR( sequences: List[CasnSequence], sequence: CasnSequence ): Boolean = {
    val sequenceStatus = sequence.status
    sequenceStatus match {
      case CasnSeqUndecided => { // PreLocking
        val op = sequence.preLockOp
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
              // we have priority, attempt pre lock
              updatePreLock( sequence, op, target, currentLockValue, currentLock )
              processTR( sequences, sequence )
            } else processTR( sequence :: sequences, currentLockSequence ) // help out
          } else {
            // currently locked
            if ( areWeBlockingCurrentLockSequence( sequence :: sequences, currentLockSequence ) ) {
              updatePreLock( sequence, op, target, currentLockValue, currentLock )
              processTR( sequences, sequence )
            } else processTR( sequence :: sequences, currentLockSequence ) // help out
          }
        }
      }
      case CasnSeqPreLocked => { // Locking
        val op = sequence.lockOp
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
            processTR( sequences, sequence )
          } else {
           // we should have at least a pre lock on the chain, help who ever is in front of us
           processTR( sequence :: sequences, currentLockSequence )
          }
        }
      }
      case CasnSeqLocked => { // Updating
        val op = sequence.updateOp
        op.opStatus match {
          case CasnOpUndecided => {
            op.execute( sequence ) // this should set the op status, if not then there was an unexpected state
            processTR( sequences, sequence ) // in any case keep going
          } 
          case CasnOpSuccess => {
            updatingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case CasnOpFailure => {
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
        val target = op.getTarget
        val currentLockValue = target.getLockValue
        val currentLock = currentLockValue.lock
        if ( currentLock != null && currentLock.sequence == sequence )
          target.updateLockValue( currentLockValue, new CasnLockValue( currentLock.next ) )
        releasingNextStep( sequence, sequenceStatus, op )
        processTR( sequences, sequence )
      }
      case CasnSeqSuccessReleased | CasnSeqFailureReleased => sequences match {
        case head :: tail => processTR( tail, head )
        case Nil => unlink( sequence, sequence.firstOp )
      }
      case _ => throw new RuntimeException( "What Happened!" ) 
    }
  }

  @tailrec
  private final def onLockChain( lock: CasnLock, sequence: CasnSequence ): Boolean = {
    if ( lock == null ) false
    else if ( lock.sequence == sequence ) true
    else onLockChain( lock.next, sequence )
  }

  @tailrec
  private final def onLockChainBefore( lock: CasnLock, sequence: CasnSequence, before: CasnSequence ): Boolean = {
    if ( lock == null ) false
    else if ( lock.sequence == sequence ) true
    else if ( lock.sequence == before ) false
    else onLockChainBefore( lock.next, sequence, before )
  }

  private final def areWeBlockingCurrentLockSequence( sequences: List[CasnSequence], currentSequence: CasnSequence ): Boolean = {
    val firstOp = currentSequence.firstOp
    if ( firstOp == null ) false
    else areWeBlockingCurrentSequenceLock( sequences, currentSequence, firstOp, firstOp.getTarget.getLockValue.lock )
  }

  @tailrec
  private final def areWeBlockingCurrentSequenceLock( sequences: List[CasnSequence], currentSequence: CasnSequence, op: CasnOp, lock: CasnLock ): Boolean = {
    if ( lock == null || lock.sequence == currentSequence ) {
      val nextOp = op.nextOp
      if ( nextOp == null ) false
      else areWeBlockingCurrentSequenceLock( sequences, currentSequence, nextOp, nextOp.getTarget.getLockValue.lock )
    }
    else if ( sequences.contains( lock.sequence ) ) true
    else areWeBlockingCurrentSequenceLock( sequences, currentSequence, op, lock.next )
  }
}

final class CasnSafeSequence extends CasnSequence {
  override def copy(): CasnSequence = {
    copy0( new CasnSafeSequence(), firstOp )
  }

  override def copy( targets: List[CasnVar] ): CasnSequence = {
    copy0( new CasnSafeSequence(), firstOp, targets )
  }

  override def cas(target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }

  override def cas(target: CasnVar, expect: ( CasnOp ) => Any, update: CasnFun): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }

  override def cas(target: CasnVar, expect: CasnFun, update: ( CasnOp ) => Any): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }

  override def cas(target: CasnVar, expect: CasnFun, update: CasnFun): CasnSequence = {
    if ( update.isInstanceOf[CasnSafeFun] && expect.isInstanceOf[CasnSafeFun] ) super.cas( target, expect, update )
    else throw new UnsupportedOperationException( "This is a safe sequence. Use only CasnSafeFun parameters." )
  }

  override def set(target: CasnVar, update: ( CasnOp ) => Any): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }

  override def set(target: CasnVar, update: CasnFun): CasnSequence = {
    if ( update.isInstanceOf[CasnSafeFun] ) super.set( target, update )
    else throw new UnsupportedOperationException( "This is a safe sequence. Use only CasnSafeFun parameters." )
  }

  override def expect(target: CasnVar, expect: CasnFun): CasnSequence = {
    if ( expect.isInstanceOf[CasnSafeFun] ) super.expect( target, expect )
    else throw new UnsupportedOperationException( "This is a safe sequence. Use only CasnSafeFun parameters." )
  }

  override def expect(target: CasnVar, expect: ( CasnOp ) => Any): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }

  override def casTagged(target: CasnVar, expect: TaggedValue, update: CasnFun): CasnSequence = {
    if ( update.isInstanceOf[CasnSafeFun] ) super.casTagged( target, expect, update )
    else throw new UnsupportedOperationException( "This is a safe sequence. Use only CasnSafeFun parameters." )
  }

  override def casTagged(target: CasnVar, expect: TaggedValue, update: ( CasnOp ) => Any): CasnSequence = {
    throw new UnsupportedOperationException( "This is a safe sequence. Functions are not allowed. Use only CasnSafeFun parameters." )
  }
}

  