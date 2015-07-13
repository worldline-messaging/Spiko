package com.worldline.mts.fifo.manager

import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class FifoAdminParams(
    command: Option[String] = None,
    fifo: Option[String] = None)
    
object FifoAdmin extends App {
  val config = ConfigFactory.load().getConfig("fifo-manager")
  
  val parser = new scopt.OptionParser[FifoAdminParams]("admfifo") {
    head("admfifo", "0.1")
    cmd("create") action { (x, c) => c.copy(command = Some("create")) } text("create a fifo.") children (
      opt[String]('f', "fifo") action { (x, c) =>
      c.copy(fifo = Some(x)) } required() text("fifo is the fifo name")
    )
    cmd("purge") action { (x, c) => c.copy(command = Some("purge")) } text("purge a fifo.") children (
      opt[String]('f', "fifo") action { (x, c) =>
      c.copy(fifo = Some(x)) } required() text("fifo is the fifo name")
    )
    cmd("drop") action { (x, c) => c.copy(command = Some("drop")) } text("drop a fifo.") children (
      opt[String]('f', "fifo") action { (x, c) =>
      c.copy(fifo = Some(x)) } required() text("fifo is the fifo name")
    )
    cmd("size") action { (x, c) => c.copy(command = Some("size")) } text("get size of a fifo.") children (
      opt[String]('f', "fifo") action { (x, c) =>
      c.copy(fifo = Some(x)) } required() text("fifo is the fifo name")
    )
    cmd("metadata") action { (x, c) => c.copy(command = Some("metadata")) } text("get metadata of a fifo.") children (
      opt[String]('f', "fifo") action { (x, c) =>
      c.copy(fifo = Some(x)) } required() text("fifo is the fifo name")
    )
  }
  
  val params = parser.parse(args, FifoAdminParams()).getOrElse {
    sys.exit(1)
  }
  
  params.command match {
    case Some("create") => create(params.fifo.get)
    case Some("purge") => purge(params.fifo.get)
    case Some("drop") => drop(params.fifo.get)
    case Some("size") => size(params.fifo.get)
    case Some("metadata") => metadata(params.fifo.get)
    case Some(_) => println("Invalid command")
    case None => println("Invalid command")
  }
  
  def create (fifoName:String) {
    val fifo = new AeroFifo(fifoName,config)
    fifo.initialize()
    Await.result(fifo.createQueue(),Duration.Inf)
  }
  
  def purge (fifoName:String) {
    val fifo = new AeroFifo(fifoName,config)
    fifo.initialize()
    Await.result(fifo.emptyQueue(),Duration.Inf)
  }
  
  def drop (fifoName:String) {
    val fifo = new AeroFifo(fifoName,config)
    fifo.initialize()
    Await.result(fifo.dropQueue(),Duration.Inf)  
  }
  
  def size (fifoName:String) {
    val fifo = new AeroFifo(fifoName,config)
    fifo.initialize()
    println(fifo.getSize())
  }
  
  def metadata(fifoName:String) {
    val fifo = new AeroFifo(fifoName,config)
    fifo.initialize()
    println("[head="+fifo.metacache.head+" ,tail="+fifo.metacache.tail+" ,maxSize="+fifo.metacache.maxSize+"]")
  }
}