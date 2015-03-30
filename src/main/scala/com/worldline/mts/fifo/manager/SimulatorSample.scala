package com.worldline.mts.fifo.manager

import akka.actor.Actor
import com.typesafe.config.Config
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.Stash
import akka.pattern.pipe
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit
import scala.util.Random
import scala.util.{Failure,Success}
import akka.util.Timeout
import scala.concurrent.Future
import scala.annotation.tailrec
import com.codahale.metrics.Gauge

case class DeviceDisconnected()
case class DeviceConnected()
case class AddMessage(payload:String)
case class SendDevice(payload:String)
case class PurgeFifo()
case class Awaken()

object CellSimulator {
	def props(deviceId:String, config:Config): Props = Props(new CellSimulator(deviceId,config))
}

object Statistics {
	private val metrics = new MetricRegistry()
	val messagesin = metrics.meter("messagesIn")
	val messagesout = metrics.meter("messagesOut")
	val evicted = metrics.meter("evicted")
	val illegalCounter0 = metrics.meter("illegalCounter0")
	val illegalCounter2 = metrics.meter("illegalCounter2")
	val illegalCounterOther = metrics.meter("illegalCounterOther")
	var connectedDevices = 0
	var stashedMessages = 0
	
	metrics.register("connectedDevices", new Gauge[Int]() {
	    def getValue():Int = {
	        connectedDevices
	    }
	})
	
	metrics.register("stashedMessages", new Gauge[Int]() {
	    def getValue():Int = {
	        stashedMessages
	    }
	})
	
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
	def incrConnectedDevices {
		connectedDevices = connectedDevices + 1
	}
	
	def decrConnectedDevices {
		connectedDevices = connectedDevices - 1
	}
}

class CellSimulator (deviceId:String, config:Config) extends Actor with Stash {
	
	var oldCounter = 0
	val fifo = new AeroFifo(deviceId,config)
	fifo.initialize
	try { Await.result(fifo.dropQueue(),Duration.Inf) } catch { case e:Throwable =>  }
	Await.result(fifo.createQueue(),Duration.Inf)
	
	def receive = disconnected
	
	def connected(): Receive = {	//Le device est connecté
		case DeviceDisconnected => {
			//println(deviceId+" disconnects in connected")
	  	  	context.become(disconnected)
	  	  	Statistics.decrConnectedDevices
		}
	  	case AddMessage(payload) =>
	  	  	val p = sender
	  	  	if(fifo.getSize>0) {	//Un nouveau message arrive mais la fifo est non vide
	  	  		stash				//On sauvegarde le message en cours
	  	  		Statistics.stashedMessages = Statistics.stashedMessages + 1
	  	  		//println(deviceId+" purging in connected")
    			context.become(purging) //Et on purge
    			self ! PurgeFifo
	  	  	} else {				//Un nouveau message arrive mais la fifo est non vide
	  	  		sendToDevice(payload)	//On envoit le message directement au device
	  	  	}
	  	  	Future.successful(Unit).pipeTo(p)
	  	case Awaken =>
	}
	
	def disconnected(): Receive = { //Le device est déconnecté
    	case DeviceConnected => { 
    		//println(deviceId+" reconnects in disconnected")
    		context.become(connected)
    		Statistics.incrConnectedDevices
    	}
    	case AddMessage(payload) => //Un nouveau message arrive mais le device est deconnecté
    	  	val p = sender
    	  	storeMessage(payload).pipeTo(p)
    	case Awaken =>
  	}
	
	def purging(): Receive = {
	  	case PurgeFifo =>
	  	  if(fifo.getSize>0) {
	  		  Await.result(fifo.pollMessage, Duration.Inf) match {
	  		      case Some(payload) => 
	  		      		Statistics.messagesout.mark
	  		      		sendToDevice(new String(payload))
	  		      		if(fifo.getSize>0)
			  	    	  self ! PurgeFifo
			  	    	else {
			  	    	  //println("Fifo is now empty")
			  	    	  self ! DeviceConnected
			  	    	}
	  		      case None => {
			  		  Statistics.messagesout.mark
			  		  Statistics.evicted.mark()
			  		  if(fifo.getSize>0) {
			  	    	  self ! PurgeFifo
			  		  } else {
			  	    	  //println("Fifo is now empty")
			  	    	  self ! DeviceConnected
			  	      }
			  	  }
	  		  }
		  	  /*val f = fifo.pollMessage
			  f.onSuccess {
			      case Some(payload) => {
			    	  Statistics.messagesout.mark
			    	  sendToDevice(new String(payload))
			  	      if(fifo.getSize>0)
			  	    	  self ! PurgeFifo
			  	      else {
			  	    	  //println("Fifo is now empty")
			  	    	  self ! DeviceConnected
			  	      }
			  	  }
			  	  case None => {
			  		  Statistics.messagesout.mark
			  		  Statistics.evicted.mark()
			  		  if(fifo.getSize>0) {
			  	    	  self ! PurgeFifo
			  		  } else {
			  	    	  //println("Fifo is now empty")
			  	    	  self ! DeviceConnected
			  	      }
			  	  }
		  	  }*/
	  	  }
	  	case AddMessage(payload) =>
	  	  	stash
	  	  	Statistics.stashedMessages = Statistics.stashedMessages + 1
	  	  	val p = sender
/*	  	  	if(fifo.getSize>0) {	//Un nouveau message arrive mais la fifo est non vide
	  	  		stash				//On sauvegarde le message en cours
	  	  	} else {				//Un nouveau message arrive mais la fifo est non vide
	  	  		unstashAll
	  	  		sendToDevice(payload)	//On envoit le message directement au device
	  	  		context.unbecome
	  	  	}*/
	  	  	Future.successful(Unit).pipeTo(p)
	  	case DeviceConnected => 	
	  	  	//println(deviceId+" is already connected")//When we purge it's because the device is connected
	  	  	context.become(connected)
	  	  	unstashAll
	  	  	Statistics.stashedMessages = 0
	  	case DeviceDisconnected => {
	  		//println(deviceId+" disconnects in purging")
	  		context.become(disconnected)
	  		unstashAll
	  		Statistics.decrConnectedDevices
	  		Statistics.stashedMessages = 0
	  	}
	  	case Awaken =>
	}
	
	def sendToDevice(payload:String) {
		val old = oldCounter
		val cur = payload.toInt
		oldCounter=cur
		if(cur-old==0) {
			Statistics.illegalCounter0.mark()
		} else if(cur-old==2) {
			Statistics.illegalCounter2.mark()
		} else if(cur-old!=1){
			Statistics.illegalCounterOther.mark()
		} else {
			
		}
	}
	
	def storeMessage(payload:String):Future[Unit] = {
		//println(payload.toInt)
		val f = fifo.addMessage(payload.getBytes())
		f.onSuccess { case _ =>
			Statistics.messagesin.mark
		}
		f
	}
}

case class Device (cell:ActorRef,count:Int,connected:Boolean,lastStatus:Long,nextStatus:Int)

object SimulatorSample extends App {
	val numDevices = 1000
	val percentConnectedDevice = 0.5
	val numMessages = 10000000
	val toggleChangeStatusMin = 5000
	val toggleChangeStatusMax = 30000
	
	var devices: Map[String,Device] = Map.empty
	
	var config = ConfigFactory.load().getConfig("fifo-manager")
	
	val system = ActorSystem("fifo-example")
	
	val messagesize = 4096 - 10
	
	implicit val timeout = Timeout(5000 seconds)
	
	val frame = String.format("%0"+messagesize+"d", int2Integer(0))
	
	Statistics.connectedDevices = 0
	
	for( i <- 1 to numDevices) {
		println("Initialize device "+i)
		val device = system.actorOf(CellSimulator.props("devicell-"+i,config), "device-"+i)
		devices = devices + (("device-"+i) -> new Device(device,0,false,
												System.currentTimeMillis(),
												Random.nextInt(toggleChangeStatusMax-toggleChangeStatusMin)+toggleChangeStatusMin))
		println("Device "+i+" is initialized:"+devices("device-"+i))
	}

	Statistics.reporter.start(10, TimeUnit.SECONDS)
	
	for( m <- 1 to numMessages) {
		val did = (m%numDevices)+1
		val device = devices("device-"+did)
		Await.result(device.cell ? AddMessage(frame+(String.format("%010d", int2Integer(device.count+1)))),Duration.Inf)
		if(System.currentTimeMillis() >= (device.lastStatus+device.nextStatus)) {
			//println("It's time to change status for device "+did+" with "+m+" messages")
			if(device.connected) {
				while (Statistics.stashedMessages > 100000) {
					Thread.sleep(1000)
					device.cell ! DeviceConnected
				}
				devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,false,
													System.currentTimeMillis(),
													Random.nextInt(toggleChangeStatusMax-toggleChangeStatusMin)+toggleChangeStatusMin))
				//println("Device "+did+" is updated:"+devices("device-"+did))
				device.cell ! DeviceDisconnected
			} else {
				devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,true,
													System.currentTimeMillis(),
													(Random.nextInt(toggleChangeStatusMax-toggleChangeStatusMin)+toggleChangeStatusMin)*2))
				//println("Device "+did+" is updated:"+devices("device-"+did))
				device.cell ! DeviceConnected
			}
		} else {
			devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,device.connected,
													device.lastStatus,
													device.nextStatus))
		}
	}
	
	
	for( i <- 1 to numDevices) {
		val device = devices("device-"+i)
		println("Device "+i+" has "+device.count+" messages")
	}
	Statistics.reporter.report()
	Statistics.reporter.stop
	Statistics.reporter.close
	
	Thread.sleep(10000)
	
	system.shutdown()
}