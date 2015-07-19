case class Email(address: String, text: String)
case class SendEmailJob(id: Int, email: Email)
sealed trait DeliveryStatus
case class EmailSent(id: Int) extends DeliveryStatus
case class EmailFailed(id: Int) extends DeliveryStatus
case object MailSenderRegistration

