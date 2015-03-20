
package com.worldline.mts.fifo.manager

import akka.actor._
import akka.pattern.pipe
import akka.pattern.ask
import akka.persistence._
import scala.concurrent.Future
import com.typesafe.config.Config
import scala.util.{Failure,Success}
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global

/*
 * A class for the metadata:
 * The queue name (qName) is the key
 * There's a head where we can read data
 * 		It is increased when we remove an element
 * There's a tail where we can write data
 * 		It is decreased when we add an element
 * There's potentially a maximum size. by default there's no limit (0)
 */
case class Metadata(qName:String, head:Long = 0, tail:Long = 0, maxSize:Int = 0)

/*
 * Some specific exceptions
 */
class AlreadyExistingQueueException (qName:String) extends Exception
class NotExistingQueueException (qName:String) extends Exception
class ReachMaxSizeException (qName:String) extends Exception
class EmptyQueueException (qName:String) extends Exception


abstract class Fifo (tsconfig: Config) {
	def initialize (): Unit
	def createQueue(qName: String, qSize:Int): Future[Unit]
	def dropQueue(qName: String): Future[Unit]
	def getSize(qName: String): Long
	def addMessage(qName: String, message:Array[Byte]): Future[Unit]
	def pollMessage(qName: String): Future[Option[Array[Byte]]]
	def peekMessage(qName: String): Future[Option[Array[Byte]]]
	def removeMessage(qName: String): Future[Unit]
	def destroy (): Unit
}

/*
 * The global FifoManager.
 * It relies on Akka (Configuration, Serialization jobs, and Actor for serializing actions)
 */
class FifoManager extends Actor {
	import com.worldline.mts.fifo.FifoProtocol._
		
	var config = context.system.settings.config.getConfig("fifo-manager")
	
	val clazz = config.getString("class")

	val fifo = Class.forName(clazz).getConstructors()(0).newInstance(config).asInstanceOf[Fifo]
	
    /*
     * Initialize the cache of the metadata
     */
    override def preStart() {
		super.preStart
		fifo.initialize
	}
	
    def receive: Receive = {
	    case cmd:CreateQueue =>
	      val p = sender
	      val f = fifo.createQueue(cmd.name, cmd.maxSize) map {
	        _ => CommandSuccess
	      } recover {
	      	case e => CommandFailure(1)
	      } pipeTo (p)
	    case cmd:DropQueue =>
	      val p = sender
	      fifo.dropQueue(cmd.name) map {
	        _ => CommandSuccess
	      } recover {
	      	case e => CommandFailure(1)
	      } pipeTo (p)
	    case cmd:AddMessage =>
	      val p = sender
	      fifo.addMessage(cmd.queue, cmd.message) recover {
	      	case e => CommandFailure(1)
	      } pipeTo (p)
	    case cmd:PollMessage =>
	      val p = sender
	      fifo.pollMessage(cmd.queue) recover {
	      	case e => CommandFailure(1)
	      } pipeTo (p)
	    case cmd:PeekMessage =>
	      val p = sender
	      fifo.peekMessage(cmd.queue) recover {
	      	case e => CommandFailure(1)
	      } pipeTo (p)
	    case cmd:GetSize =>
	      val p = sender
	      try {
	    	  p ! fifo.getSize(cmd.queue)
	      } catch {
	          case t:Throwable => p ! CommandFailure(1)
	      }
	}
  
    
}

object FifoExample extends App {
	import com.worldline.mts.fifo.FifoProtocol._
	import akka.util.Timeout
	
	val system = ActorSystem("fifo-example")
	val fifoManager = system.actorOf(Props[FifoManager], "fifo-example-1")
	
	implicit val timeout = Timeout(5000 seconds)
	
	val fdrop = fifoManager ? new DropQueue("fifo-1")
	fdrop.map {
	  	case CommandSuccess => println("Fifo is dropped")
	  	case CommandFailure(_) => println("Fifo has not been dropped")
	}
	Await.result(fdrop, Duration.Inf)
	
    val fcreate = fifoManager ? new CreateQueue("fifo-1")
	fcreate.map {
	  	case CommandSuccess => println("Fifo is created")
	  	case CommandFailure(_) => println("Fifo has not been created")
	}
	Await.result(fcreate, Duration.Inf)
	
	val fadd1 = fifoManager ? AddMessage("fifo-1", "test1".getBytes())
	fadd1.map {
		case CommandFailure(_) => println("Message 1 is not inserted")
	  	case _ => println("Message 1 is inserted")
	}
	Await.result(fadd1, Duration.Inf)
	
	val fadd2 = fifoManager ? AddMessage("fifo-1", "test2".getBytes())
	fadd2.map {
		case CommandFailure(_) => println("Message 2 is not inserted")
	  	case _ => println("Message 2 is inserted")
	}
	Await.result(fadd2, Duration.Inf)
	
	val fadd3 = fifoManager ? AddMessage("fifo-1", "test3".getBytes())
	fadd3.map {
		case CommandFailure(_) => println("Message 3 is not inserted")
	  	case _ => println("Message 3 is inserted")
	}
	Await.result(fadd3, Duration.Inf)
	
	val fsize1 = fifoManager ? GetSize("fifo-1")
	fsize1.map {
		case CommandFailure(_) => println("Size is not retrieved")
	  	case size => println("Size :"+size)
	}
	Await.result(fsize1, Duration.Inf)
	
	val fpeek1 = fifoManager ? PeekMessage("fifo-1")
	fpeek1.map {
		case CommandFailure(_) => println("Message is not retrieved")
		case Some(message) => println("peek :"+new String(message.asInstanceOf[Array[Byte]]))
	}
	Await.result(fpeek1, Duration.Inf)
	
	val fsize2 = fifoManager ? GetSize("fifo-1")
	fsize2.map {
		case CommandFailure(_) => println("Size is not retrieved")
	  	case size => println("Size :"+size)
	}
	Await.result(fsize2, Duration.Inf)
	
	val fpoll1 = fifoManager ? PollMessage("fifo-1")
	fpoll1.map {
		case CommandFailure(_) => println("Message is not retrieved")
		case Some(message) => println("poll :"+new String(message.asInstanceOf[Array[Byte]]))
	}
	Await.result(fpoll1, Duration.Inf)
	
	val fsize3 = fifoManager ? GetSize("fifo-1")
	fsize3.map {
		case CommandFailure(_) => println("Size is not retrieved")
	  	case size => println("Size :"+size)
	}
	Await.result(fsize3, Duration.Inf)
	
	val fpoll2 = fifoManager ? PollMessage("fifo-1")
	fpoll2.map {
		case CommandFailure(_) => println("Message is not retrieved")
		case Some(message) => println("poll :"+new String(message.asInstanceOf[Array[Byte]]))
	}
	Await.result(fpoll2, Duration.Inf)
	
	val fsize4 = fifoManager ? GetSize("fifo-1")
	fsize4.map {
		case CommandFailure(_) => println("Size is not retrieved")
	  	case size => println("Size :"+size)
	}
	Await.result(fsize4, Duration.Inf)
	
	val fpoll3 = fifoManager ? PollMessage("fifo-1")
	fpoll3.map {
		case CommandFailure(_) => println("Message is not retrieved")
		case Some(message) => println("poll :"+new String(message.asInstanceOf[Array[Byte]]))
	}
	Await.result(fpoll3, Duration.Inf)
	
	val fsize5 = fifoManager ? GetSize("fifo-1")
	fsize5.map {
		case CommandFailure(_) => println("Size is not retrieved")
	  	case size => println("Size :"+size)
	}
	Await.result(fsize5, Duration.Inf)
	
	val fpoll4 = fifoManager ? PollMessage("fifo-1")
	fpoll4.map {
		case CommandFailure(_) => println("Message is not retrieved")
		case Some(message) => println("poll :"+new String(message.asInstanceOf[Array[Byte]]))
	}
	Await.result(fpoll4, Duration.Inf)
	
	system.shutdown()
}
