package anomalyStruct

class AnomalyData(tStart: String, tStop: String, per: Int, obs: Int){
    val timestampStart = tStart
    val timestampStop = tStop
    val period = per
    val observations = obs

    override def toString = {
      s"Start: $timestampStart, Stop: $timestampStop, period: $period, observations: $observations"
    }
}
