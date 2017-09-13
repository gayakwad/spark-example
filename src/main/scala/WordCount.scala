import java.time.Instant

import org.apache.spark._
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME"))

    val now = Instant.now
    val eventsList = List(
      Event(now.toEpochMilli, 1, "send_message", 1, 1),
      Event(now.plusSeconds(1).toEpochMilli, 2, "send_message", 1, 2),
      Event(now.plusSeconds(1).toEpochMilli, 2, "send_message", 1, 3))

    val events: RDD[Event] = sc.parallelize(eventsList)

    val tuples = events
      .map(e => (e.user_id, e))
      .reduceByKey { (x, y) => Event(x.timestamp, x.user_id, x.event + y.event, x.channel_id, y.message_id) }
      .collect()

    tuples.foreach(x => println(x))

  }
}