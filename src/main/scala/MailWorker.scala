import akka.actor.{ActorRef, ActorLogging, Actor}
import concurrent.duration._

class MailWorker(master: ActorRef) extends Actor with ActorLogging {

  master ! MailSenderRegistration

  implicit val ec = context.dispatcher

  def receive = {
    case s: SendEmailJob => fakeEmailSend(s)
  }

  def fakeEmailSend(emailJob: SendEmailJob) = {
    val replyTo = sender()
    log.info(s"sent email to ${emailJob.email.address}")
    context.system.scheduler.scheduleOnce(250.millis, replyTo, EmailSent(emailJob.id))
  }
}
