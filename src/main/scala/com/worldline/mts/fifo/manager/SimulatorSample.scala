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

case class DeviceDisconnected()
case class DeviceConnected()
case class AddMessage(payload:String)
case class PurgeFifo()
case class Awaken()

object CellSimulator {
	def props(deviceId:String, config:Config): Props = Props(new CellSimulator(deviceId,config))
}

object Statistics {
	private val metrics = new MetricRegistry()
	val messagesin = metrics.meter("messagesIn")
	val messagesout = metrics.meter("messagesOut")
	
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
}

class CellSimulator (deviceId:String, config:Config) extends Actor with Stash {
	
	var oldCounter = 0
	val fifo = new AeroFifo(deviceId,config)
	fifo.initialize
	try { Await.result(fifo.createQueue(),Duration.Inf) } catch { case e:Throwable =>  }
	//Await.result(fifo.createQueue(),Duration.Inf)
	
	def receive = idle
	
	def idle: Receive = {
		case DeviceConnected => {
			println(deviceId+" reconnects in idle")
	  	  	context.become(connected)
		}
	  	case DeviceDisconnected => {
	  		println(deviceId+" disconnects in idle")
	  	  	context.become(disconnected)
	  	}
	  	case Awaken =>
	}
	
	def connected(): Receive = {	//Le device est connecté
		case DeviceDisconnected => {
			println(deviceId+" disconnects in connected")
	  	  	context.become(disconnected)
		}
	  	case AddMessage(payload) =>
	  	  	val p = sender
	  	  	if(fifo.getSize>0) {	//Un nouveau message arrive mais la fifo est non vide
	  	  		stash				//On sauvegarde le message en cours
	  	  		println(deviceId+" purging in connected")
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
    		println(deviceId+" reconnects in disconnected")
    		context.become(connected)
    	}
    	case AddMessage(payload) => //Un nouveau message arrive mais le device est deconnecté
    	  	val p = sender
    	  	storeMessage(payload)
    	  	Future.successful(Unit).pipeTo(p)
    	case Awaken =>
  	}
	
	def purging(): Receive = {
	  	case PurgeFifo =>
	  	  val f = fifo.pollMessage
		  f.onSuccess {
		      case Some(payload) => {
		    	  sendToDevice(new String(payload))
		    	  Statistics.messagesout.mark
		  	      if(fifo.getSize>0)
		  	    	  self ! PurgeFifo
		  	      else {
		  	    	  println("Fifo is now empty")
		  	    	  self ! DeviceConnected
		  	      }
		  	  }
		  	  case None => {
		  		  if(fifo.getSize>0)
		  	    	  self ! PurgeFifo
		  	      else {
		  	    	  println("Fifo is now empty")
		  	    	  self ! DeviceConnected
		  	      }
		  	  }
	  	  }
	  	  
	  	case AddMessage(payload) =>
	  	  	stash
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
	  	  	println(deviceId+" is already connected")//When we purge it's because the device is connected
	  	  	context.become(connected)
	  	  	unstashAll
	  	case DeviceDisconnected => {
	  		println(deviceId+" disconnects in purging")
	  		context.become(disconnected)
	  		unstashAll
	  	}
	  	case Awaken =>
	}
	
	
	def sendToDevice(payload:String) {
	}
	
	def storeMessage(payload:String) {
		//println(payload.toInt)
		Await.result(fifo.addMessage(payload.getBytes()),Duration.Inf)
		Statistics.messagesin.mark
	}
}

case class Device (cell:ActorRef,count:Int,connected:Boolean,lastStatus:Long,nextStatus:Int)

object SimulatorSample extends App {
	val numDevices = 100000
	val percentConnectedDevice = 0.5
	val numMessages = 100000000
	val toggleChangeStatusMin = 5000
	val toggleChangeStatusMax = 30000
	
	var devices: Map[String,Device] = Map.empty
	
	var config = ConfigFactory.load().getConfig("fifo-manager")
	
	val system = ActorSystem("fifo-example")
	
	implicit val timeout = Timeout(5000 seconds)
	
	for( i <- 1 to numDevices) {
		println("Initialize device "+i)
		val device = system.actorOf(CellSimulator.props("device-"+i,config), "device-"+i)
		device ! DeviceDisconnected
		devices = devices + (("device-"+i) -> new Device(device,0,false,
												System.currentTimeMillis(),
												Random.nextInt(toggleChangeStatusMax-toggleChangeStatusMin)+toggleChangeStatusMin))
		println("Device "+i+" is initialized:"+devices("device-"+i))
	}

	Statistics.reporter.start(10, TimeUnit.SECONDS)
	
	for( m <- 1 to numMessages) {
		val did = (m%numDevices)+1
		val device = devices("device-"+did)
		Await.result(device.cell ? AddMessage(String.format("%01024d", int2Integer(m))),5000 seconds)
		if(System.currentTimeMillis() >= (device.lastStatus+device.nextStatus)) {
			println("It's time to change status for device "+did+" with "+m+" messages")
			if(device.connected) {
				devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,false,
													System.currentTimeMillis(),
													Random.nextInt(toggleChangeStatusMax-toggleChangeStatusMin)+toggleChangeStatusMin))
				println("Device "+did+" is updated:"+devices("device-"+did))
				device.cell ! DeviceDisconnected
			} else {
				devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,true,
													System.currentTimeMillis(),
													toggleChangeStatusMax))
				println("Device "+did+" is updated:"+devices("device-"+did))
				device.cell ! DeviceConnected
			}
		} else {
			devices = devices + (("device-"+did) -> new Device(device.cell,device.count+1,device.connected,
													device.lastStatus,
													device.nextStatus))
		}
		//Thread.sleep(1)
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