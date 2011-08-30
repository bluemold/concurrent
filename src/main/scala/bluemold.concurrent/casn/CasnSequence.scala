package bluemold.concurrent.casn

import org.bluemold.unsafe.Unsafe
import annotation.tailrec

object CasnVar {
  import Unsafe._

  val valueIndex = objectDeclaredFieldOffset( classOf[CasnVar], "value" )
  val lockValueIndex = objectDeclaredFieldOffset( classOf[CasnVar], "lockValue" )

  val opStatusIndex = objectDeclaredFieldOffset( classOf[Op], "opStatus" ) 
  val prevValueIndex = objectDeclaredFieldOffset( classOf[Op], "prevValue" ) 
  val updateValueIndex = objectDeclaredFieldOffset( classOf[Op], "updateValue" ) 
  val expectValueIndex = objectDeclaredFieldOffset( classOf[Op], "expectValue" ) 
  val preRevertValueIndex = objectDeclaredFieldOffset( classOf[Op], "preRevertValue" ) 

  val sequenceConstructedIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "constructed" ) 
  val sequenceStatusIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "status" ) 

  val sequencePreLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "preLockOp" ) 
  val sequenceLockOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "lockOp" ) 
  val sequenceUpdateOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "updateOp" ) 
  val sequenceRevertOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "revertOp" ) 
  val sequenceReleaseOpIndex = objectDeclaredFieldOffset( classOf[CasnSequence], "releaseOp" ) 

  def create(): CasnVar = create( null )
  def create( value: Any ): CasnVar = new CasnVar().initialize( new TaggedValue( value ) )
}

abstract class CasnOp {
  def prior( index: Int ): Any
  def fail()
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

class CasnVar {
  @volatile var value: TaggedValue = new TaggedValue( null )
  @volatile var lockValue: CasnLockValue = new CasnLockValue( null )

  private[casn] def initialize( value: TaggedValue ): CasnVar = { this.value = value; this }

  private[casn] def getLockValue = lockValue

  private[casn] def updateTagged( expect: TaggedValue, update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.valueIndex, expect, update )

  private[casn] def updateLockValue( expect: CasnLockValue, update: CasnLockValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.lockValueIndex, expect, update )
  
  def get: Any = value.value
  def getTagged: TaggedValue = value
  def safeGet(): Any = new CasnSequence().executeAndGet( this )
  def safeGetTagged(): TaggedValue = new CasnSequence().executeAndGetTagged( this )
  def set( value: Any ) { new CasnSequence().setVal( this, value ).execute() }
  def cas( expect: Any, update: Any ): Boolean = new CasnSequence().casVal( this, expect, update ).execute()
}

class TaggedValue( _value: Any ) {
  val value = _value
}
private[casn] class CasnLockValue( _lock: CasnLock ) {
  val lock = _lock
}
private[casn] case class CasnLock( target: CasnVar, sequence: CasnSequence, next: CasnLock, isPreLock: Boolean )

private[casn] class CasnStatus
private[casn] object CasnUndecided extends CasnStatus
private[casn] object CasnPreLocked extends CasnStatus
private[casn] object CasnLocked extends CasnStatus
private[casn] object CasnAborted extends CasnStatus
private[casn] object CasnSuccess extends CasnStatus
private[casn] object CasnFailure extends CasnStatus
private[casn] object CasnReverted extends CasnStatus
private[casn] object CasnSuccessReleased extends CasnStatus
private[casn] object CasnFailureReleased extends CasnStatus

private[casn] case class Op( target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any ) extends CasnOp {
  @volatile var nextOp: Op = null
  @volatile var prevOp: Op = null
  @volatile var prevValue: TaggedValue = null
  @volatile var updateValue: TaggedValue = null
  @volatile var expectValue: TaggedValue = null
  @volatile var preRevertValue: TaggedValue = null
  @volatile var opStatus: CasnStatus = CasnUndecided
  def isUndecided = opStatus == CasnUndecided
  def isSuccess = opStatus == CasnSuccess
  def isFailure = opStatus == CasnFailure || opStatus == CasnReverted
  def isReverted = opStatus == CasnReverted
  
  private[casn] def setPrevValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.prevValueIndex, null, update )

  private[casn] def setExpectValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.expectValueIndex, null, update )

  private[casn] def setUpdateValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.updateValueIndex, null, update )

  private[casn] def setPreRevertValue( update: TaggedValue ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.preRevertValueIndex, null, update )

  private[casn] def updateStatus( expect: CasnStatus, update: CasnStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.opStatusIndex, expect, update )

  def copy(): Op = Op( target, expect, update )
  
  def copy( target: CasnVar ): Op = Op( target, expect, update )

  @tailrec
  final override def fail() {
    opStatus match {
      case CasnUndecided => if ( updateStatus( CasnUndecided, CasnFailure ) ) return else fail()
      case CasnSuccess => return
      case CasnFailure => return
      case CasnReverted => return
    }
  }

  def prior(index: Int): Any = {
    if ( index < 0 ) null
    else if ( index == 0 ) {
      val prevValue = this.prevValue
      if ( prevValue != null ) prevValue.value
      else null
    } else {
      val prevOp = this.prevOp
      if ( prevOp != null ) prevOp.prior( index - 1 )
      else null
    }
  } 

  def execute( sequence: CasnSequence ) {
    executeTR( sequence )
  }

  @tailrec
  private final def executeTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnUndecided => {
          val prevValue = this.prevValue
          val expectValue = this.expectValue
          val updateValue = this.updateValue
          if ( prevValue == null ) {
            setPrevValue( target.getTagged )
            executeTR( sequence ) // next step
          } else if ( expectValue == null ) {
            setExpectValue( new TaggedValue( expect( this ) ) )
            executeTR( sequence ) // next step
          } else if ( updateValue == null ) {
            setUpdateValue( new TaggedValue( update( this ) ) )
            executeTR( sequence ) // next step
          } else {
            val currentValue = target.getTagged
            if ( currentValue == prevValue ) {
              if ( currentValue.value == expectValue.value ) {
                target.updateTagged( currentValue,  updateValue ) // update target
                executeTR( sequence ) // next step
              } else { // fail
                if ( ! updateStatus( CasnUndecided, CasnFailure ) )
                  executeTR( sequence ) // unexpected
              }
            } else {
              if ( currentValue == updateValue ) {
                if ( ! updateStatus( CasnUndecided, CasnSuccess ) )
                  executeTR( sequence ) // unexpected
                else executeTR( sequence ) // tail recursion test
              } else executeTR( sequence ) // unexpected
            }
          }
        }
        case CasnSuccess =>
        case CasnFailure =>
        case CasnReverted =>
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnAborted =>
      case CasnSuccess =>
      case CasnFailure =>
      case CasnSuccessReleased =>
      case CasnFailureReleased =>
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  def revert( sequence: CasnSequence ) {
    revertTR( sequence )
  }

  @tailrec
  private final def revertTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnSuccess => {
          val preRevertValue = this.preRevertValue
          if ( preRevertValue == null ) {
            setPreRevertValue( target.getTagged )
            revertTR( sequence ) // next step
          } else {
            val currentValue = target.getTagged
            if ( currentValue == preRevertValue ) {
              if ( target.updateTagged( preRevertValue, new TaggedValue( prevValue.value ) ) ) { // success
                if ( updateStatus( CasnSuccess, CasnReverted ) ) return
                else revertTR( sequence ) // unexpected
              } else revertTR( sequence ) // unexpected
            } else revertTR( sequence ) // unexpected
          }
        }
        case CasnFailure =>
        case CasnReverted =>
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSuccess =>
      case CasnFailure =>
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] case class Get( targetPrime: CasnVar ) extends Op( targetPrime, _ => null, _ => null ) {
  override def execute( sequence: CasnSequence ) {
    executeTR( sequence )
  }
  @tailrec
  private final def executeTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnUndecided => {
          val prevValue = this.prevValue
          if ( prevValue == null ) {
            setPrevValue( target.getTagged )
            executeTR( sequence ) // next step
          } else {
            if ( updateStatus( CasnUndecided, CasnSuccess ) ) return
            else executeTR( sequence ) // unexpected
          }
        }
        case CasnSuccess =>
        case CasnFailure =>
        case CasnReverted =>
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSuccess =>
      case CasnFailure =>
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence ) {
    revertTR( sequence )
  }
  @tailrec
  private final def revertTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnSuccess => {
          if ( updateStatus( CasnSuccess, CasnReverted ) ) return
          else revertTR( sequence ) // unexpected
        }
        case CasnFailure => return
        case CasnReverted => return
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSuccess => return
      case CasnFailure => return
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

private[casn] case class ExpectTagged( targetPrime: CasnVar, expectTagged: TaggedValue ) extends Op( targetPrime, _ => null, _ => null ) {
  override def execute( sequence: CasnSequence ) {
    executeTR( sequence )
  }
  @tailrec
  private final def executeTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnUndecided => {
          val prevValue = this.prevValue
          if ( prevValue == null ) {
            setPrevValue( target.getTagged )
            executeTR( sequence ) // next step
          } else {
            if ( prevValue == expectTagged ) {
              if ( updateStatus( CasnUndecided, CasnSuccess ) ) return
              else executeTR( sequence ) // unexpected
            } else { // fail
              if ( updateStatus( CasnUndecided, CasnFailure ) ) return
              else executeTR( sequence ) // unexpected
            } 
          }
        }
        case CasnSuccess => return
        case CasnFailure => return
        case CasnReverted => return
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSuccess => return
      case CasnFailure => return
      case _ => throw new RuntimeException( "What Happened" )
    }
  }

  override def revert( sequence: CasnSequence ) {
    revertTR( sequence )
  }
  @tailrec
  private final def revertTR( sequence: CasnSequence ) {
    sequence.status match {
      case CasnLocked => opStatus match {
        case CasnSuccess => {
          if ( updateStatus( CasnSuccess, CasnReverted ) ) return
          else revertTR( sequence ) // unexpected
        }
        case CasnFailure => return
        case CasnReverted => return
        case _ => throw new RuntimeException( "What Happened" )
      }
      case CasnSuccess => return
      case CasnFailure => return
      case _ => throw new RuntimeException( "What Happened" )
    }
  }
}

class CasnSequence {
  @volatile var firstOp: Op = null
  @volatile var lastOp: Op = null
  @volatile var constructed: Int = 0
  @volatile var status: CasnStatus = CasnUndecided
  @volatile var preLockOp: Op = null
  @volatile var lockOp: Op = null
  @volatile var updateOp: Op = null
  @volatile var revertOp: Op = null
  @volatile var releaseOp: Op = null

  def isUndecided = status == CasnUndecided || status == CasnPreLocked || status == CasnLocked
  def isPreLocked = status == CasnPreLocked
  def isLocked = status == CasnLocked
  def isSuccess = status == CasnSuccess
  def isFailure = status == CasnFailure

  private def setNextPreLockOp( expect: Op, update: Op ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequencePreLockOpIndex, expect, update )

  private def setNextLockOp( expect: Op, update: Op ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequenceLockOpIndex, expect, update )

  private def setNextUpdateOp( expect: Op, update: Op ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequenceUpdateOpIndex, expect, update )

  private def setNextRevertOp( expect: Op, update: Op ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequenceRevertOpIndex, expect, update )

  private def setNextReleaseOp( expect: Op, update: Op ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequenceReleaseOpIndex, expect, update )

  private def updateStatus( expect: CasnStatus, update: CasnStatus ): Boolean =
    Unsafe.compareAndSwapObject( this, CasnVar.sequenceStatusIndex, expect, update )

  private def hasPriorityOver( other: CasnSequence ): Boolean = {
    if ( this == other ) false
    else Unsafe.naturalReferenceCompare( this, other ) == -1
  }
  
  // Only meant to be called by a single thread prior to
  // the sequence being added to the concurrent environment
  // via the execute method by that same thread
  private def addOp0( op: Op ) {
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
    if ( constructed == 0 )
      addOp0( Get( target ) )
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
    if ( constructed == 0 ) {
      addOp0( ExpectTagged( target, expect ) )
      addOp0( Op( target, ( op: CasnOp ) => op.prior( 0 ), update ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def casTagged( target: CasnVar, expect: TaggedValue, update: CasnFun ) = {
    if ( constructed == 0 ) {
      addOp0( ExpectTagged( target, expect ) )
      addOp0( Op( target, ( op: CasnOp ) => op.prior( 0 ), ( op: CasnOp ) => update.get( op ) ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def casTaggedVal( target: CasnVar, expect: TaggedValue, update: Any ) = {
    if ( constructed == 0 ) {
      addOp0( ExpectTagged( target, expect ) )
      addOp0( Op( target, ( op: CasnOp ) => op.prior( 0 ), _ => update ) )
    } else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  def expectTagged( target: CasnVar, expect: TaggedValue ) = {
    if ( constructed == 0 )
      addOp0( ExpectTagged( target, expect ) )
    else
      throw new IllegalStateException( "sequence already constructed" )
    this
  }

  private def addOp( target: CasnVar, expect: ( CasnOp ) => Any, update: ( CasnOp ) => Any ) = {
    if ( constructed == 0 )
      addOp0( Op( target, expect, update ) )
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
  protected final def copy0( sequence: CasnSequence, op: Op ): CasnSequence = {
    if ( op != null ) {
      sequence.addOp0( op.copy() )
      copy0( sequence, op.nextOp )
    } else sequence
  }

  @tailrec
  protected final def copy0( sequence: CasnSequence, op: Op, targets: List[CasnVar] ): CasnSequence = {
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
    if ( Unsafe.compareAndSwapInt( this, CasnVar.sequenceConstructedIndex, 0, 1 ) ) {
      if ( firstOp != null )
        process( this )
      else true
    } else false
  }

  def getLast: Any = {
    if ( constructed == 0 )
      throw new IllegalStateException( "sequence not executed yet" )
    if ( status != CasnSuccess )
      throw new IllegalStateException( "sequence did not complete successfully" )
    if ( firstOp == null || lastOp == null )
      throw new IllegalStateException( "no operations defined!" )
    get0( lastOp, 0 )
  }

  def get( index: Int ): Any = {
    if ( index < 0 )
      throw new IllegalArgumentException( "index out of bounds" )
    if ( constructed == 0 )
      throw new IllegalStateException( "sequence not executed yet" )
    if ( status != CasnSuccess )
      throw new IllegalStateException( "sequence did not complete successfully" )
    if ( firstOp == null || lastOp == null )
      throw new IllegalStateException( "no operations defined!" )
    get0( firstOp, index )
  }
    
  @tailrec
  private final def get0( op: Op, index: Int ): Any = {
    if ( op != null ) {
      if ( index < 0 )
        throw new RuntimeException( "What Happened!" )
      else if ( index == 0 ) {
        val prevValue = op.prevValue
        if ( prevValue == null )
          throw new RuntimeException( "What Happened!" )
        prevValue.value
      } else get0( op.nextOp, index - 1 )
    } else throw new IllegalArgumentException( "index out of bounds" )
  }

  def executeAndGet( target: CasnVar ): Any = {
    if ( constructed == 0 ) {
      addOp0( Get( target ) )
      if ( Unsafe.compareAndSwapInt( this, CasnVar.sequenceConstructedIndex, 0, 1 ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val prevValue = lastOp.prevValue
            if ( prevValue != null ) prevValue.value
            else throw new IllegalStateException( "sequence did not complete successfully" )
          } else throw new IllegalStateException( "sequence did not complete successfully" )
        } else throw new IllegalStateException( "no operations defined!" )
      } else throw new IllegalStateException( "sequence already constructed" )
    } else throw new IllegalStateException( "sequence already constructed" );
  }

  def executeAndGetTagged( target: CasnVar ): TaggedValue = {
    if ( constructed == 0 ) {
      addOp0( Get( target ) )
      if ( Unsafe.compareAndSwapInt( this, CasnVar.sequenceConstructedIndex, 0, 1 ) ) {
        if ( firstOp != null && lastOp != null ) {
          if ( process( this ) ) {
            val prevValue = lastOp.prevValue
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

  private final def updatePreLock( sequence: CasnSequence, op: Op, target: CasnVar, currentLockValue: CasnLockValue, currentLock: CasnLock ) = {
    if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock, true ) ) ) ) {
      preLockingNextStep( sequence, op )
    }
  }

  private final def preLockingNextStep( sequence: CasnSequence, op: Op ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextPreLockOp( op, nextOp )
    else sequence.updateStatus( CasnUndecided, CasnPreLocked )
  }

  private final def updateLock( sequence: CasnSequence, op: Op, target: CasnVar, currentLockValue: CasnLockValue, currentLock: CasnLock ) = {
    if ( target.updateLockValue( currentLockValue, new CasnLockValue( CasnLock( target, sequence, currentLock.next, false ) ) ) ) {
      lockingNextStep( sequence, op )
    }
  }

  private final def lockingNextStep( sequence: CasnSequence, op: Op ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextLockOp( op, nextOp )
    else sequence.updateStatus( CasnPreLocked, CasnLocked )
  }

  private final def updatingNextStep( sequence: CasnSequence, op: Op ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextUpdateOp( op, nextOp )
    else sequence.updateStatus( CasnLocked, CasnSuccess )
  }

  private final def revertingNextStep( sequence: CasnSequence, op: Op ) = {
    val prevOp = op.prevOp
    if ( prevOp != null ) setNextRevertOp( op, prevOp )
    else sequence.updateStatus( CasnAborted, CasnFailure )
  }

  private final def releasingNextStep( sequence: CasnSequence, sequenceStatus: CasnStatus, op: Op ) = {
    val nextOp = op.nextOp
    if ( nextOp != null ) setNextReleaseOp( op, nextOp )
    else if ( sequenceStatus == CasnSuccess ) sequence.updateStatus( CasnSuccess, CasnSuccessReleased )
    else sequence.updateStatus( CasnFailure, CasnFailureReleased )
  }

  @tailrec
  private final def unlink( sequence: CasnSequence, op: Op ): Boolean = {
    if ( op == null )
      sequence.status == CasnSuccessReleased
    else {
      op.prevOp = null
      unlink( sequence, op.nextOp )
    }
  }

  @tailrec
  private final def processTR( sequences: List[CasnSequence], sequence: CasnSequence ): Boolean = {
    sequence.status match {
      case CasnUndecided => { // PreLocking
        val op = sequence.preLockOp
        val target = op.target
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
      case CasnPreLocked => { // Locking
        val op = sequence.lockOp
        val target = op.target
        val currentLockValue = op.target.getLockValue
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
      case CasnLocked => { // Updating
        val op = sequence.updateOp
        op.opStatus match {
          case CasnUndecided => {
            op.execute( sequence ) // this should set the op status, if not then there was an unexpected state
            processTR( sequences, sequence ) // in any case keep going
          } 
          case CasnSuccess => {
            updatingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case CasnFailure => {
            val prevOp = op.prevOp
            if ( prevOp != null ) {
              setNextRevertOp( null, prevOp )
              sequence.updateStatus( CasnLocked, CasnAborted )
              processTR( sequences, sequence )
            } else {
              sequence.updateStatus( CasnLocked, CasnFailure )
              processTR( sequences, sequence )
            }
          }
          case CasnReverted => {
            // unexpected, already in reverted pipe, try again
            processTR( sequences, sequence )
          } 
        }
      }
      case CasnAborted => { // Reverting
        val op = sequence.revertOp
        op.opStatus match {
          // the CasnUndecided and CasnFailure case shouldn't occur on op after this method is called
          case CasnSuccess => {
            op.revert( sequence ) // this should set the op status, if not then there was an unexpected state
            processTR( sequences, sequence )
          }
          case CasnReverted => {
            revertingNextStep( sequence, op )
            processTR( sequences, sequence )
          }
          case _ => throw new RuntimeException( "What Happened!" )
        }
      }
      case CasnSuccess => { // Releasing
        val op = sequence.releaseOp
        val target = op.target
        val currentLockValue = target.getLockValue
        val currentLock = currentLockValue.lock
        if ( currentLock != null && currentLock.sequence == sequence )
          target.updateLockValue( currentLockValue, new CasnLockValue( currentLock.next ) )
        releasingNextStep( sequence, CasnSuccess, op )
        processTR( sequences, sequence )
      }
      case CasnFailure => { // Releasing
        val op = sequence.releaseOp
        val target = op.target
        val currentLockValue = target.getLockValue
        val currentLock = currentLockValue.lock
        if ( currentLock != null && currentLock.sequence == sequence )
          target.updateLockValue( currentLockValue, new CasnLockValue( currentLock.next ) )
        releasingNextStep( sequence, CasnFailure, op )
        processTR( sequences, sequence )
      }
      case CasnSuccessReleased => sequences match {
        case head :: tail => processTR( tail, head )
        case Nil => unlink( sequence, sequence.firstOp )
      }
      case CasnFailureReleased => sequences match {
        case head :: tail => processTR( tail, head )
        case Nil => unlink( sequence, sequence.firstOp )
      }
      case _ => throw new RuntimeException( "What Happened!" ) 
    }
  }

  private final def process( sequences: List[CasnSequence] ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    sequenceStatus match {
      case CasnUndecided => prelocking( sequences, sequence.firstOp )
      case CasnPreLocked => locking( sequences, sequence.firstOp )
      case CasnLocked => updating( sequences )
      case CasnSuccess => releasing( sequences, sequence.lastOp )
      case CasnFailure => releasing( sequences, sequence.lastOp )
      case _ => throw new RuntimeException( "What Happened!" ) 
    }
  }

  private final def updatePreLock( sequences: List[CasnSequence], op: Op, currentLockValue: CasnLockValue, sequence: CasnSequence ): Boolean = {
    val target = op.target
    if ( target.updateLockValue( currentLockValue, new CasnLockValue(
          CasnLock( target, sequence, currentLockValue.lock, true )
        ) ) )
      prelocking( sequences, op.nextOp )
    else
      prelocking( sequences, op ) // unexpected state, re-run step
  }

  @tailrec
  private final def prelocking( sequences: List[CasnSequence], op: Op ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    sequenceStatus match {
      case CasnUndecided => {
        if ( op == null ) { // we've prelocked them all, attempt update status to PreLocked
            if ( sequence.updateStatus( CasnUndecided, CasnPreLocked ) ) 
              locking( sequences, sequence.firstOp )
            else
              prelocking( sequences, op ) // unexpected state, re-run step
        } else { 
          val currentLockValue = op.target.getLockValue
          val currentLock = currentLockValue.lock
          if ( currentLock == null ) { // if not currently locked or pre locked then attempt pre lock
            updatePreLock( sequences, op, currentLockValue, sequence )
          } else { 
            val currentSequence = currentLock.sequence
            if ( sequence == currentSequence || onLockChain( currentLock.next, sequence ) ) {
               // already ( prelocked or locked ) move on to the next step
              prelocking( sequences, op.nextOp )
            } else if ( currentLock.isPreLock ) { // if currently pre locked
              if ( sequence.hasPriorityOver( currentSequence ) ) { // and we have priority then attempt pre lock
                updatePreLock( sequences, op, currentLockValue, sequence )
              } else process( currentSequence :: sequences ) // otherwise help out
            } else { // if currently locked
              if ( areWeBlockingCurrentLockSequence( sequences, currentSequence ) ) {
                updatePreLock( sequences, op, currentLockValue, sequence )
              } else process( currentSequence :: sequences ) // otherwise help out
            }
          }
        }
      }
      case CasnPreLocked => locking( sequences, sequence.firstOp )
      case CasnLocked => updating( sequences )
      case CasnSuccess => releasing( sequences, sequence.lastOp )
      case CasnFailure => releasing( sequences, sequence.lastOp )
      case _ => throw new RuntimeException( "What Happened!" )
    }
  }

  private final def updateLock( sequences: List[CasnSequence], op: Op, currentLockValue: CasnLockValue, sequence: CasnSequence ): Boolean = {
    val currentLock = currentLockValue.lock
    // This check may be unnecessary
    if ( currentLock == null )
      throw new RuntimeException( "What Happened!" )
    val target = op.target
    if ( target.updateLockValue( currentLockValue, new CasnLockValue(
          CasnLock( target, sequence, currentLock.next, false )
        ) ) )
      locking( sequences, op.nextOp )
    else
      locking( sequences, op ) // unexpected state, re-run step
  }

  @tailrec
  private final def locking( sequences: List[CasnSequence], op: Op ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    sequenceStatus match {
      // the CasnUndecided case shouldn't occur after this method is called
      // case CasnUndecided => prelocking( sequences, sequence.firstOp ) 
      case CasnPreLocked => {
        if ( op == null ) { // we've locked them all, attempt to update status to Locked
            if ( sequence.updateStatus( CasnPreLocked, CasnLocked ) ) 
              updating( sequences )
            else
              locking( sequences, op ) // unexpected state, re-run step
        } else {
          val currentLockValue = op.target.getLockValue
          val currentLock = currentLockValue.lock
          if ( currentLock == null )
            // throw new RuntimeException( "What Happened!" )
            locking( sequences, op ) // unexpected state, re-run step
          else {
            val currentSequence = currentLock.sequence
            if ( sequence == currentSequence ) { // if we have an immediate lock on it
              if ( currentLock.isPreLock ) { // and we have a pre lock then attempt to replace pre lock with lock
                updateLock( sequences, op, currentLockValue, sequence )
              } else locking( sequences, op.nextOp ) // otherwise already locked, move on to the next one
            } else {
             // we should have at least a pre lock on the chain, help who ever is in front of us
             process( currentSequence :: sequences )
            }
          }
        }
      }
      case CasnLocked => updating( sequences )
      case CasnSuccess => releasing( sequences, sequence.lastOp )
      case CasnFailure => releasing( sequences, sequence.lastOp )
      case _ => throw new RuntimeException( "What Happened!" )
    }
  }

  @tailrec
  private final def updating( sequences: List[CasnSequence] ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    sequenceStatus match {
      // the CasnUndecided and CasnPreLocked cases shouldn't occur after this method is called
      case CasnLocked => {
        val updateOp = sequence.updateOp
        if ( updateOp == null ) {
          sequence.setNextUpdateOp( null, sequence.firstOp )
          updating( sequences ) // continue
        } else { 
          val currentOpStatus = updateOp.opStatus
          currentOpStatus match {
            case CasnUndecided => {
              updateOp.execute( sequence ) // this should set the op status, if not then there was an unexpected state
              updating( sequences ) // in any case keep going
            } 
            case CasnSuccess => {
              val nextOp = updateOp.nextOp
              if ( nextOp != null ) { // we have more ops to go, update currentOp and keep going
                sequence.setNextUpdateOp( updateOp, nextOp ) // fail or not we don't care
                updating( sequences ) // keep going
              } else { // We have successfully updated all operations, mark this baby done!!!!
                if ( sequence.updateStatus( CasnLocked, CasnSuccess ) )
                  releasing( sequences, sequence.lastOp )
                else
                  updating( sequences ) // unexpected state, re-run step
              }
            }
            case CasnFailure => reverting( sequences, updateOp.prevOp )
            case CasnReverted => reverting( sequences, updateOp.prevOp )
          }
        }
      }
      case CasnSuccess => releasing( sequences, sequence.lastOp )
      case CasnFailure => releasing( sequences, sequence.lastOp )
      case _ => throw new RuntimeException( "What Happened!" )
    }
  }

  @tailrec
  private final def reverting( sequences: List[CasnSequence], op: Op ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    sequenceStatus match {
      // the CasnUndecided and CasnPreLocked and CasnSuccess cases shouldn't occur after this method is called
      case CasnLocked => {
        op.opStatus match {
          // the CasnUndecided and CasnFailure case shouldn't occur on op after this method is called
          case CasnSuccess => {
            op.revert( sequence ) // this should set the op status, if not then there was an unexpected state
            reverting( sequences, op ) // in any case keep going
          }
          case CasnReverted => {
            val prevOp = op.prevOp
            if ( prevOp != null ) { // we have more ops to go
              reverting( sequences, prevOp ) // keep going, revert the prevOp
            } else { // We have successfully reverted all operations, mark this baby a failure!!!!
              if ( sequence.updateStatus( CasnLocked, CasnFailure ) )
                releasing( sequences, sequence.lastOp )
              else
                reverting( sequences, op ) // unexpected state, re-run step
            }
          }
        }
      }
      case CasnFailure => releasing( sequences, sequence.lastOp )
      case _ => throw new RuntimeException( "What Happened!" )
    }
  }

  @tailrec
  private final def releasing( sequences: List[CasnSequence], op: Op ): Boolean = {
    val sequence = sequences match { case head :: tail => head case Nil => throw new RuntimeException( "What Happened!" ) }
    val sequenceStatus = sequence.status
    if ( sequenceStatus == CasnSuccess || sequenceStatus == CasnFailure ) {
      if ( op == null ) {
        // were done releasing for this sequence, now unlink ops for efficient garbage collection 
        unlinking( sequences, sequence.firstOp )
      } else {
        val target = op.target
        val currentLockValue = target.getLockValue
        val currentLock = currentLockValue.lock
        if ( currentLock == null || currentLock.sequence != sequence ) {
          // if not currently locking this entity then move on to the next one
          releasing( sequences, op.prevOp )
        } else {
          if ( target.updateLockValue( currentLockValue, new CasnLockValue( currentLock.next ) ) )
            releasing( sequences, op.prevOp )
          else
            releasing( sequences, op ) // unexpected state, re-run step
        }
      }
    } else { // the CasnUndecided and CasnPreLocked and CasnSuccess cases shouldn't occur after this method is called
      throw new RuntimeException( "What Happened!" )
    }
  }
  
  @tailrec
  private final def unlinking( sequences: List[CasnSequence], op: Op ): Boolean = {
    if ( op == null ) {
      // were done unlinking for this sequence, backout of processing it 
      sequences match {
        case sequence :: Nil => {
          // we are at the end and our original sequence has been processed, return the result
          if ( sequence == this ) isSuccess
          else throw new RuntimeException( "What Happened!" )
        }
        case sequence :: tail => process( tail )
        case Nil => throw new RuntimeException( "What Happened!" )
      }
    } else {
      op.prevOp = null
      unlinking( sequences, op.nextOp )
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
    else areWeBlockingCurrentSequenceLock( sequences, currentSequence, firstOp, firstOp.target.getLockValue.lock )
  }

  @tailrec
  private final def areWeBlockingCurrentSequenceLock( sequences: List[CasnSequence], currentSequence: CasnSequence, op: Op, lock: CasnLock ): Boolean = {
    if ( lock == null || lock.sequence == currentSequence ) {
      val nextOp = op.nextOp
      if ( nextOp == null ) false
      else areWeBlockingCurrentSequenceLock( sequences, currentSequence, nextOp, nextOp.target.getLockValue.lock )
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

  