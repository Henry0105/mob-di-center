package com.youzu.mob.sink

import java.io.ByteArrayOutputStream
import java.util.Properties

import javax.activation.{DataHandler, DataSource}
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import javax.mail.{Message, Session, Transport}
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import scala.collection.mutable

class MailSink2(properties: Properties,
  from: String,
  to: Array[String],
  subject: String,
  body: String) extends Sink {
  protected val session: Session = Session.getInstance(properties)
  protected val message: MimeMessage = new MimeMessage(session)

  def send(): Unit = {
    val transport: Transport = session.getTransport
    transport.connect("smtp-internal.mob.com",587,"bigdata_alert", "Mob20201211!")
    transport.sendMessage(message, message.getAllRecipients)
    transport.close()
  }

  def init(): Unit = {
    message.setFrom(new InternetAddress(from))
    message.setSubject(subject)
    message.setContent(body, "text/html; charset=utf-8")
    println(message.getContentType)
   // message.setText(body)
    to.foreach(addr => message.addRecipient(Message.RecipientType.TO, new InternetAddress(addr)))
  }

}

class MailSink2WithAttachments(properties: Properties,
  from: String,
  to: Array[String],
  subject: String,
  body: String)
  extends {
    protected val multipart = new MimeMultipart()
    protected val toCloseStreams = new mutable.ListBuffer[ByteArrayOutputStream]()
  } with MailSink2(properties, from, to, subject, body) {

  override def init(): Unit = {
    super.init()
    val bodyPart = new MimeBodyPart()
  //  bodyPart.setText(body)
    bodyPart.setContent(body, "text/html; charset=utf-8")
    multipart.addBodyPart(bodyPart)
  }

  def attach(filename: String, byteArrayOutputStream: ByteArrayOutputStream): Unit = {
    attach(filename, byteArrayOutputStream.toByteArray)
  }

  def attach(filename: String, array: Array[Byte]): Unit = {
    val tmp = new MimeBodyPart()
    val source: DataSource = new ByteArrayDataSource(array, "application/octet-stream")
    tmp.setDataHandler(new DataHandler(source))
    tmp.setFileName(filename)
    multipart.addBodyPart(tmp)
  }

  def attach(filename: String, wb: SXSSFWorkbook): Unit = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream()
    toCloseStreams.append(out)
    wb.write(out)
    attach(filename, out.toByteArray)
  }

  override def send(): Unit = {
    try {
     message.setContent(multipart, "text/html;charset=utf-8")
      println(message.getContentType)
      super.send()
    } finally {
      toCloseStreams.foreach(_.close())
    }
  }
}