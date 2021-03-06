package test.bluemold.concurrent

import junit.framework._;
import Assert._
import bluemold.concurrent.AtomicBoolean
;

object AtomicBooleanTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[NonLockingHashMapTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

class AtomicBooleanTest extends TestCase("app") {

    /**
     * Rigourous Tests :-)
     */
    def testBasics() {
      val a1 = AtomicBoolean.create()
      assertFalse(a1.get());
      val a2 = AtomicBoolean.create( false )
      assertFalse(a2.get());
      val a3 = AtomicBoolean.create( true )
      assertTrue(a3.get());
    }
}
