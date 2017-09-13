import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

class StreamingTest extends FunSuite with StreamingSuiteBase {

  test("CountByWindow with windowDuration 3s and slideDuration=2s") {
    // There should be 2 windows :  {batch2, batch1},  {batch4, batch3, batch2}
    val batch1 = List("a", "b")
    val batch2 = List("d", "f", "a")
    val batch3 = List("f", "g", " h")
    val batch4 = List("a")
    val input = List(batch1, batch2, batch3, batch4)
    val expected = List(List(5L), List(7L))

    def countByWindow(ds: DStream[String]): DStream[Long] = {
      ds.countByWindow(windowDuration = Seconds(3), slideDuration = Seconds(2))
    }

    testOperation[String, Long](input, countByWindow _, expected, ordered = true)
  }

}
