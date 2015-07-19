import akka.actor.{Props, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import concurrent.duration._

object ResizableStreamBalancerApp {

  def main(args: Array[String]): Unit = {
    
    var i = 0

    def getEmailAddress = s"#${i +=1; i}"
    def getEmailText = s"body #${util.Random.nextInt(100)}"
    def emails = Iterator.continually(Email(getEmailAddress, getEmailText)).toStream

    implicit val system = ActorSystem("resizable-streams-example")
    implicit val materializer = ActorMaterializer()
    implicit val ec = system.dispatcher

    val emailSource = Source(emails)

    val actor = emailSource.runWith(Sink.actorSubscriber(MailMaster.props))

    system.actorOf(Props(new MailWorker(actor)))
    
    system.scheduler.schedule(2.second, 2.seconds) {
      system.actorOf(Props(new MailWorker(actor)))
    }
  }
}