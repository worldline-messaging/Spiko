package com.worldline.mts.fifo.manager

import com.typesafe.config.Config
import java.util.concurrent.Executors
import scala.concurrent.Await
import com.tapad.aerospike.ScanFilter
import com.tapad.aerospike.AerospikeClient
import java.nio.ByteBuffer
import scala.concurrent.Future
import java.util.concurrent.ThreadFactory
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import com.tapad.aerospike.ClientSettings
import com.tapad.aerospike.DefaultValueMappings._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit


/*
 * Akka configuration
 */
class AeroFifoConfig(tsconfig: Config) {
	val urls = tsconfig.getStringList("urls")
	val namespace = tsconfig.getString("namespace")
	val metadataset = tsconfig.getString("metadataset")
	val messageset = tsconfig.getString("messageset")
	val selectorThreads = tsconfig.getInt("selectorThreads")
}

class AeroFifo (tsconfig: Config) extends Fifo (tsconfig) {
	
	val config = new AeroFifoConfig(tsconfig)
	
	var metacache: Map[String, Metadata] = Map.empty
	
	//Aerospike client Initialization
	val asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
		override def newThread(runnable: Runnable) = {
                        val thread = new Thread(runnable)
                        thread.setDaemon(true)
                        thread
                }
    })
	val client = AerospikeClient(config.urls,new ClientSettings(blockingMode=true,selectorThreads=config.selectorThreads, taskThreadPool=asyncTaskThreadPool))
    val metadataset = client.namespace(config.namespace).set[String,Array[Byte]](config.metadataset) //We hope that this Set should always be in the buffer cache 
    val messageset = client.namespace(config.namespace).set[String,Array[Byte]](config.messageset)
    
	def initialize (): Unit = {
		//println("initialize the cache")
		val bins = Seq ("head","tail","maxsize")
		val filter = new ScanFilter [String, Map[String, Array[Byte]]] {
			def filter(key: String, record:Map[String, Array[Byte]]): Boolean = { true }
		}
		val records = Await.result(metadataset.scanAllRecords[List](bins,filter),Duration.Inf)
	    records.foreach(r => {
	      //println (r)
	      val md = buildMetadata(r._1,r._2)
	      metacache = metacache + (md.qName -> md)
	    })
	    //println("metacache="+metacache)
	}
	
	def destroy (): Unit = {}
	
	private def buildMetadata(qName:String, row:Map[String, Array[Byte]]): Metadata = {
		new Metadata(
			qName, 
			ByteBuffer.wrap(row("head")).getLong(),
			ByteBuffer.wrap(row("tail")).getLong(),
			ByteBuffer.wrap(row("maxsize")).getInt()
	    )
	}
	
	/*
     * create a queue asynchronously.
     * update the metadata in store and in cache
     */
	def createQueue(qName: String, qSize:Int=0): Future[Unit] = {
		if(metacache.contains(qName)) {
			Future.failed(new AlreadyExistingQueueException(qName))
		} else {
			val md = new Metadata(qName,0,0,qSize)
			saveMetadata(md)
		}
	}
  
	/*
	 * save the metadata in store and in cache for a queue
	 */
	private def saveMetadata (metadata: Metadata): Future[Unit] = {
		val bins : Map[String, Array[Byte]] = Map (
		    "head" 		-> long2bytearray(metadata.head),
		    "tail" 		-> long2bytearray(metadata.tail),
		    "maxsize" 	-> int2bytearray(metadata.maxSize)
		)
		//println("create metadata "+bins+" for fifo "+metadata.qName)
		metadataset.putBins(metadata.qName, bins) andThen { case _ =>
			metacache = metacache + (metadata.qName -> metadata)
			//println("metadata="+metadata)
		}
	}
	
	/*
	 * delete the metadata from store and cache for a queue
	 */
	def dropQueue(qName: String): Future[Unit] = {
		if(!metacache.contains(qName)) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			metadataset.delete(qName) andThen { case _ =>
				metacache = metacache - qName
			}
		}
	}
  
	/*
	 * retrieve the size of a queue from the metadata in cache
	 */
	def getSize(qName: String): Long = {
		if(!metacache.contains(qName)) {
			throw new NotExistingQueueException(qName)
		} else {
			val md = metacache(qName)
			md.tail-md.head
		}
	}
	
	/*
	 * add a message in store for a queue
	 * update the tail counter in the metadata
	 */
	def addMessage(qName: String, message:Array[Byte]): Future[Unit] = {
		if(!metacache.contains(qName)) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			val md = metacache(qName)
			if(reachMaxSize(md))
				Future.failed(new ReachMaxSizeException(qName))
			val bins : Map[String, Array[Byte]] = Map ("payload" -> message)
			messageset.putBins(qName+"_"+(md.tail+1), bins).flatMap { _ => 
				val md2 = new Metadata(md.qName,md.head,md.tail+1,md.maxSize)
				saveMetadata(md2)
			}
		}
	}
  
	/*
	 * poll a message (delete) from the store for a queue
	 * update the head counter in the metadata
	 */
	def pollMessage(qName: String): Future[Option[Array[Byte]]] = {
		if(!metacache.contains(qName)) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			val md = metacache(qName)
			if(emptyFifo(md)) {
				//println("Fifo is empty")
				Future.successful(None)
			} else {
				val md2 = new Metadata(md.qName,md.head+1,md.tail,md.maxSize)
				for {
					payload <- messageset.get(qName+"_"+(md.head+1), "payload")
		    		res <- saveMetadata(md2)
				} yield payload
			}
		}
	}
  
	/*
	 * peek a message (do not delete) from the store for a queue
	 */
	def peekMessage(qName: String): Future[Option[Array[Byte]]] = {
		if(!metacache.contains(qName)) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			val md = metacache(qName)
			if(emptyFifo(md)) {
				//println("Fifo is empty")
				Future.successful(None)
			} else {
				messageset.get(qName+"_"+(md.head+1), "payload")
			}
		}
	}
	
	/*
	 * delete a message from the store for a queue without read it
	 * update the head counter in the metadata
	 */
	def removeMessage(qName: String): Future[Unit] = {
		if(!metacache.contains(qName)) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			val md = metacache(qName)
			if(emptyFifo(md)) {
				//println("Fifo is empty")
				Future.failed(new EmptyQueueException(qName))
			} else {
				val md2 = new Metadata(md.qName,md.head+1,md.tail,md.maxSize)
				saveMetadata(md2)
			}
		}
	}
	
	private def long2bytearray(lng: Long): Array[Byte] = {
	  val bb = ByteBuffer.allocate(8) //8, In java Long is 64bits
	  bb.putLong(lng)
	  bb.array()
	}
	
	private def int2bytearray(i: Int): Array[Byte] = {
	  val bb = ByteBuffer.allocate(4) //8, In java Long is 32bits
	  bb.putInt(i)
	  bb.array()
	}
	
	private def emptyFifo(md:Metadata): Boolean = {
		md.tail-md.head == 0
	}
	
	private def reachMaxSize(md:Metadata): Boolean = {
		(md.maxSize > 0 && md.tail-md.head > md.maxSize)
	}
}


object PerformanceApp extends App {
	val metrics = new MetricRegistry()
	val messagescounter = metrics.meter("messages")
		
	val reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
	var config = ConfigFactory.load().getConfig("fifo-manager")
	
	val fifo = new AeroFifo(config)
	fifo.initialize
	
	try {
		Await.result(fifo.dropQueue("perftest0"), Duration.Inf)
	} catch {
	  case t:Throwable => t.printStackTrace();
	}
	Await.result(fifo.createQueue("perftest0",-1), Duration.Inf)

	val maxmessages = 100000
	println("E C R I T U R E")
	reporter.start(10, TimeUnit.SECONDS)
	for( i <- 1 to maxmessages) {
		Await.result(fifo.addMessage("perftest0", String.format("%01024d", int2Integer(i)).getBytes()), Duration.Inf)
		messagescounter.mark()
	}
	reporter.report()
	
	val size = fifo.getSize("perftest0")
	println("size ="+size)
	if(size!=maxmessages) throw new IllegalStateException("The size is "+size)
	
	println("L E C T U R E")
	
	for( i <- 1 to maxmessages) {
		val n = Await.result(fifo.pollMessage("perftest0"), Duration.Inf) match {
		  	case None => throw new IllegalStateException("None message")
		  	case Some(arr) => new String(arr).toInt
		}
		if(i!=n) throw new IllegalStateException("Not the good number "+n)
		messagescounter.mark()
	}
	reporter.report()
	reporter.stop()
	reporter.close()
}