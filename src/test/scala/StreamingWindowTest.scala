import java.time.Instant

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

class StreamingWindowTest extends FunSuite with StreamingSuiteBase {

  // Based on - http://blog.cloudera.com/blog/2014/11/how-to-do-near-real-time-sessionization-with-spark-streaming-and-apache-hadoop/
  test("Finding sessions") {
    val MESSAGE_TEXT: String = "send_message"
    val USER_1: Int = 1
    val USER_2: Int = 2
    val CHANNEL_1: Int = 1
    val CHANNEL_2: Int = 2

    val userOneSessionStart = Instant.now.minusSeconds(1)
    val userTwoSessionStart = Instant.now.minusSeconds(1)

    val userEvents = List(List(
      // user 1
      Event(userOneSessionStart.toEpochMilli, USER_1, MESSAGE_TEXT, CHANNEL_1, 1),
      Event(userOneSessionStart.plusSeconds(1).toEpochMilli, USER_1, MESSAGE_TEXT, CHANNEL_2, 2),
      Event(userOneSessionStart.plusSeconds(2).toEpochMilli, USER_1, MESSAGE_TEXT, CHANNEL_1, 3),
      Event(userOneSessionStart.plusSeconds(4).toEpochMilli, USER_1, MESSAGE_TEXT, CHANNEL_1, 4),
      //user 2
      Event(userTwoSessionStart.toEpochMilli, USER_2, MESSAGE_TEXT, CHANNEL_1, 5),
      Event(userOneSessionStart.plusSeconds(1).toEpochMilli, USER_2, MESSAGE_TEXT, CHANNEL_2, 6),
      Event(userOneSessionStart.plusSeconds(1).toEpochMilli, USER_2, MESSAGE_TEXT, CHANNEL_2, 7)
    ))

    val expectedStats = List(List(
      UserStats(USER_1, userOneSessionStart.toEpochMilli, userOneSessionStart.plusSeconds(4).toEpochMilli, Map(CHANNEL_1 -> 3, CHANNEL_2 -> 1)),
      UserStats(USER_2, userTwoSessionStart.toEpochMilli, userTwoSessionStart.plusSeconds(1).toEpochMilli, Map(CHANNEL_1 -> 1, CHANNEL_2 -> 2))
    ))

    def sessionize(ds: DStream[Event]): DStream[UserStats] = {
      ds.map(u => (u.user_id, (u.timestamp, u.timestamp, Map(u.channel_id -> 1))))
        .reduceByKey((a, b) => (
          if (a._1 > b._1) b._1 else a._1,
          if (a._2 > b._2) a._2 else b._2,
          Util.addValues(a._3, b._3)))
        .updateStateByKey(Util.updateStatbyOfSessions)
        // adding key in UserStats Object
        .map(t => (t._1, (UserStats(t._1, t._2._1.sessionStartTime, t._2._1.sessionEndTime, t._2._1.channelToMessage), t._2._2)))
        .filter(t => System.currentTimeMillis() - t._2._1.sessionEndTime < Util.SESSION_TIMEOUT)
        .map(v => v._2._1)
    }

    testOperation[Event, UserStats](userEvents, sessionize _, expectedStats, ordered = true)
  }
}