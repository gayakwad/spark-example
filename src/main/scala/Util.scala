object Util {

  //https://forums.databricks.com/questions/369/how-do-i-handle-a-task-not-serializable-exception.html
  val SESSION_TIMEOUT = 10000 // TIME_OUT + SPARK STREAMING BATCH WINDOW

  def updateStatbyOfSessions(
                              //(sessionStartTime, sessionFinishTime, countOfEvents)
                              a: Seq[(Long, Long, Map[Int, Int])],
                              //(sessionStartTime, sessionFinishTime, countOfEvents, isNewSession)
                              b: Option[(UserStats, Boolean)]
                            ): Option[(UserStats, Boolean)] = {

    //This function will return a Optional value.
    //If we want to delete the value we can return a optional "None".
    //This value contains four parts
    //(startTime, endTime, countOfEvents, isNewSession)
    var result: Option[(UserStats, Boolean)] = null

    // These if statements are saying if we didn’t get a new event for
    //this session’s ip address for longer then the session
    //timeout + the batch time then it is safe to remove this key value
    //from the future Stateful DStream
    if (a.isEmpty) {
      if (System.currentTimeMillis() - b.get._1.sessionStartTime < SESSION_TIMEOUT + 1000) {
        result = None
      } else {
        if (!b.get._2) {
          result = b
        } else {
          result = Some((UserStats(0, b.get._1.sessionStartTime, b.get._1.sessionEndTime, b.get._1.channelToMessage), false))
        }
      }
    }

    //Now because we used the reduce function before this function we are
    //only ever going to get at most one event in the Sequence.
    a.foreach(c => {
      if (b.isEmpty) {
        //If there was no value in the Stateful DStream then just add it
        //new, with a true for being a new session
        result = Some((UserStats(0, c._1, c._2, c._3), true))
      } else {
        if (c._1 - b.get._1.sessionStartTime < SESSION_TIMEOUT) {
          //If the session from the stateful DStream has not timed out
          //then extend the session
          result = Some(
            UserStats(0,
              Math.min(c._1, b.get._1.sessionStartTime), //newStartTime
              Math.max(c._2, b.get._1.sessionEndTime), //newFinishTime
              Util.addValues(b.get._1.channelToMessage, c._3)),
            false //This is not a new session
          )
        } else {
          //Otherwise remove the old session with a new one
          result = Some((UserStats(0,
            c._1, //newStartTime
            c._2, //newFinishTime
            b.get._1.channelToMessage), //newSumOfEvents
            true //new session
          ))
        }
      }
    })
    result
  }

  def addValues(a: Map[Int, Int], b: Map[Int, Int]): Map[Int, Int] = {
    //https://stackoverflow.com/questions/7076128/best-way-to-merge-two-maps-and-sum-the-values-of-same-key
    a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0)) }
  }

}
