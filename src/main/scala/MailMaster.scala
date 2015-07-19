import akka.stream.OverflowStrategy
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl._
import akka.pattern.ask
import akka.actor.{Props, ActorRef, Terminated}
import akka.util.Timeout
import concurrent.duration._

class MailMaster extends ActorSubscriber with ImplicitMaterializer {

  var workersCount = 0
  val parallelism = 1
  def maxQueueSize = workersCount * parallelism

  var mailQueue = Map.empty[Int, SendEmailJob]
  var currentEmailId = 0

  def getNewId = {
    val current = currentEmailId
    currentEmailId += 1
    current
  }

  override val requestStrategy = new DynamicMaxInFlightRequestStrategy {
    override def max: Int = maxQueueSize
    override def inFlightInternally: Int = mailQueue.size
  }

  def createBalancer(workers: Set[ActorRef]) = {
    import FlowGraph.Implicits._

    val flow = Flow() { implicit b =>

      val balance = b.add(Balance[SendEmailJob](workers.size))
      val merge = b.add(Merge[DeliveryStatus](workers.size))

      implicit val timeout = Timeout(2.second)
      implicit val ec = context.dispatcher
      def sendToWorker(worker: ActorRef) = Flow[SendEmailJob]
        .mapAsyncUnordered(parallelism) { emailJob =>
        (worker ? emailJob)
          .mapTo[EmailSent]
          .recover { case e => EmailFailed(emailJob.id) }
        }

      workers.foreach { worker =>
        balance ~> sendToWorker(worker) ~> merge
      }

      (balance.in, merge.out)
    }

    val source = Source.actorRef(2 * maxQueueSize, OverflowStrategy.fail)

    val sink = Sink.actorRef(self, MailMaster.Completed)

    source via flow to sink
  }

  def receive = noWorkers

  def noWorkers: Receive = {
    case OnNext(email: Email) =>
      val emailJobId = getNewId
      mailQueue += emailJobId -> SendEmailJob(emailJobId, email)
      assert(mailQueue.size <= maxQueueSize, s"queued too many: ${mailQueue.size}")

    case MailSenderRegistration =>
      val newWorker = Set(context watch sender())
      context become hasWorkers(newWorker, createBalancer(newWorker).run())
      workersCount = 1
  }

  def hasWorkers(workers: Set[ActorRef], balancer: ActorRef): Receive = {
    case Terminated(a) =>
      updateWorkers(workers - a, balancer)

    case MailSenderRegistration =>
      val newWorker = context watch sender()
      updateWorkers(workers + newWorker, balancer)

    case OnNext(email: Email) =>
      val emailJobId = getNewId
      val job = SendEmailJob(emailJobId, email)
      mailQueue += emailJobId -> job
      assert(mailQueue.size <= maxQueueSize, s"queued too many: ${mailQueue.size}")
      balancer ! job

    case EmailSent(id) =>
      mailQueue -= id

    case EmailFailed(id) =>
      mailQueue get id foreach (balancer ! _)

  }

  def updateWorkers(newWorkers: Set[ActorRef], oldBalancer: ActorRef) = {
    implicit val ec = context.dispatcher
    //shutdown old balancer
    context.system.scheduler.scheduleOnce(2.seconds, oldBalancer, akka.actor.Status.Success)

    workersCount = newWorkers.size
    if (workersCount > 0) {
      val newBalancer = createBalancer(newWorkers).run()
      context become hasWorkers(newWorkers, newBalancer)
    } else {
      context become noWorkers
    }
  }

}

object MailMaster {

  def props = Props[MailMaster]

  case object Completed

}