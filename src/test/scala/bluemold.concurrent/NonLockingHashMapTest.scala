package bluemold.concurrent

import junit.framework._;
import Assert._;

object NonLockingHashMapTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[NonLockingHashMapTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for simple App.
 */
class NonLockingHashMapTest extends TestCase("app") {

    /**
     * Rigourous Tests :-)
     */
    def testBasics() {
      val map = new NonLockingHashMap[String,String](1)

      1 to 600 foreach { i => map += (( "Hi"+i, "Bye" )) }
      1 to 500 foreach { i => map -= ("Hi"+i) }

      println( "TraverableAgain: " + map.isTraversableAgain );

      // map foreach { kv => println( "k: " + kv._1 + " v: " + kv._2 ) }
      map foreach { i => }
      1 to 2 foreach { i => println("-------"); }
      println( "Size: " + map.size )
      map.empty
      1 to 2 foreach { i => println("-------"); }
      println( "Size: " + map.size )
    }
}
