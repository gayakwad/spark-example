import org.apache.spark._
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME"))
    /*    val input = args.length match {
          case x: Int if x > 1 => sc.textFile(args(1))
          case _ => sc.parallelize(List("pandas", "i like pandas"))
        }
        val words = input.flatMap(line => line.split(" "))
        args.length match {
          case x: Int if x > 2 => {
            val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
            counts.saveAsTextFile(args(2))
          }
          case _ => {
            val wc = words.countByValue()
            println(wc.mkString(","))
          }
        }*/

    val eventsList = List(
      Event("2017-02-07T00:00:00Z", 1, "send_message", 1, 1),
      Event("2017-02-07T00:00:20Z", 2, "send_message", 1, 2),
      Event("2017-02-07T00:00:30Z", 2, "send_message", 1, 3))
    val events: RDD[Event] = sc.parallelize(eventsList)
    val tuples = events.map(e => (e.user_id, e)).reduceByKey {  (x, y) => Event(x.timestamp, x.user_id,x.event + y.event, x.channel_id, y.message_id) }.collect().map(x => println(x))


  }
}