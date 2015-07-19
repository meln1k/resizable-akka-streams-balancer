import akka.stream.actor.RequestStrategy

abstract class DynamicMaxInFlightRequestStrategy extends RequestStrategy {

  def max: Int

  def inFlightInternally: Int

  def batchSize: Int = 5

  override def requestDemand(remainingRequested: Int): Int = {
    val batch = math.min(batchSize, max)
    if ((remainingRequested + inFlightInternally) <= (max - batch))
      math.max(0, max - remainingRequested - inFlightInternally)
    else 0
  }
}