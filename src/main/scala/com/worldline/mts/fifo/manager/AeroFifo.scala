package com.worldline.mts.fifo.manager

import com.typesafe.config.Config
import java.util.concurrent.Executors
import scala.concurrent.Await
import com.tapad.aerospike.ScanFilter
import com.tapad.aerospike.AerospikeClient
import java.nio.ByteBuffer
import scala.concurrent.Future
import scala.concurrent.blocking
import java.util.concurrent.ThreadFactory
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.collection.JavaConversions._
import com.tapad.aerospike.ClientSettings
import com.tapad.aerospike.DefaultValueMappings._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit
import com.aerospike.client.AerospikeException
import com.aerospike.client.ResultCode
import scala.concurrent.ExecutionContext
import com.tapad.aerospike.WriteSettings
import com.tapad.aerospike.ClientSettings
import com.tapad.aerospike.WriteSettings
import com.tapad.aerospike.ScanFilter



/**
 * @author a140168
 * Singleton to use only one connections pool to access Aerospike
 */
object AsClient {
	private var instance:AerospikeClient = null
	
	def getInstance (implicit config: AeroFifoConfig) = {
	  	synchronized {
		  	if(instance == null) {
				val asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
					override def newThread(runnable: Runnable) = {
			                    val thread = new Thread(runnable)
			                    thread.setDaemon(true)
			                    thread
			            }
				}) 
				instance = AerospikeClient(config.urls,new ClientSettings(blockingMode=true,selectorThreads=config.selectorThreads, taskThreadPool=asyncTaskThreadPool))
		  	}
		  	instance
	  	}
	}  
}
 
/**
 * Akka configuration
 */
class AeroFifoConfig(tsconfig: Config) {
  //AeroSpike urls to connect to
	val urls = tsconfig.getStringList("urls")
	//AeroSpike namespace containing the fifos
  val namespace = tsconfig.getString("namespace")
  //AeroSpike set containing the metadata
	val metadataset = tsconfig.getString("metadataset")
  //AeroSpike set containing the fifos
	val messageset = tsconfig.getString("messageset")
  //Aerospike selector number of threads
	val selectorThreads = tsconfig.getInt("selectorThreads")
  //Consistency level
  val commitAll = tsconfig.getBoolean("commitAll")
  //Producer and consumer are not on the same jvm.
  val localOnly = tsconfig.getBoolean("localOnly")
}


/**
 * @author a140168
 * Aerospike implementation of a Fifo
 */
class AeroFifo (qName: String,tsconfig: Config) extends Fifo[Array[Byte]](qName,tsconfig) {
	
	implicit val config = new AeroFifoConfig(tsconfig)
	
	var metacache: Metadata = null
	
  val wp = WriteSettings(commitAll=config.commitAll)
  val namespace = AsClient.getInstance.namespace(config.namespace,writeSettings=wp)
  val metadataset = namespace.set[String,Array[Byte]](config.metadataset) //We hope that this Set should always be in the buffer cache 
  val messageset = namespace.set[String,Array[Byte]](config.messageset)
  /* NB: The buffer cache does not work with the SSD aerospike driver. 
   * So we have to store data in memory and on disk if we use the SSD aerospike driver  
   */
  
	def initialize (): Unit = {
		Await.result(readMetadata(),Duration.Inf)
	}
	
	def destroy (): Unit = {}
	
	/**
   * create a queue asynchronously.
   * update the metadata in store and in cache
   */
	def createQueue(qSize:Int=0): Future[Unit] = {
		if(metacache!=null) {
			Future.failed(new AlreadyExistingQueueException(qName))
		} else {
			val md = new Metadata(qName,0,0,qSize)
			saveMetadata(md)
		}
	}
  
	/*def exponentialBackoff(r: Int): Duration = scala.math.pow(2, r).round * 10 milliseconds
	
	def retry[T](f: => Future[T], backoff: (Int) => Duration = exponentialBackoff)(nMax: Int)(implicit e: ExecutionContext): Future[T] = {
		
		def recurRetry[T](f: => Future[T], backoff: (Int) => Duration = exponentialBackoff)(n: Int)(implicit e: ExecutionContext): Future[T] = {
			n match {
	        	case i if (i < nMax) => f.recoverWith{ case e: AerospikeException => {
	        		if(e.getResultCode()==ResultCode.KEY_BUSY)
	        			println("Retry "+(n+1)+" in "+backoff(n+1).toMillis+" milliseconds");blocking {Thread.sleep(backoff(n+1).toMillis)};recurRetry(f,backoff)(n + 1)}
	        	}
	        	case _ => f
			}   
		}
		
		recurRetry(f, backoff)(0)
	}*/
	
	/**
	 * save the metadata in store and in cache for a queue
	 */
	private def saveMetadata (metadata: Metadata): Future[Unit] = {
		val bins : Map[String, Array[Byte]] = Map (
		    "head" 		-> long2bytearray(metadata.head),
		    "tail" 		-> long2bytearray(metadata.tail),
		    "maxsize" 	-> int2bytearray(metadata.maxSize)
		)

		//println("Trying to save metadata :"+metadata )
		metadataset.putBins(metadata.qName, bins,Option(-1)) map { case _ =>
      metacache = metadata
      //println("metadata="+metadata)
    }
	}
	
  /**
   * save the head metadata in store and in cache for a queue
   */
  private def saveMetadataHead (metadata: Metadata): Future[Unit] = {
    val bins : Map[String, Array[Byte]] = Map (
        "head"    -> long2bytearray(metadata.head)
    )

    //println("Trying to save metadata :"+metadata )
    metadataset.putBins(metadata.qName, bins,Option(-1)) map { case _ =>
      metacache = metadata
      //println("metadata="+metadata)
    }
  }
  
  /**
   * save the tail metadata in store and in cache for a queue
   */
  private def saveMetadataTail (metadata: Metadata): Future[Unit] = {
    val bins : Map[String, Array[Byte]] = Map (
        "tail"    -> long2bytearray(metadata.tail)
    )

    //println("Trying to save metadata :"+metadata )
    metadataset.putBins(metadata.qName, bins,Option(-1)) map { case _ =>
      metacache = metadata
      //println("metadata="+metadata)
    }
  }
  
  /**
   * update the metadata cache from the aerospike server
   */
  private def readMetadata (): Future[Unit] = {
    metadataset.getBins(qName, Seq("head","tail","maxsize")) map {
      r => {
        if(r.contains("head") && r.contains("tail") && r.contains("maxsize")) {
          metacache = new Metadata(qName, 
                ByteBuffer.wrap(r("head")).getLong(),
                ByteBuffer.wrap(r("tail")).getLong(),
                ByteBuffer.wrap(r("maxsize")).getInt()
            )
        }
        //println("metacache has changed "+metacache)
      }
    }
  }
  
  /**
   * purge the queue
   */
  def emptyQueue(): Future[Unit] = {
    val md = new Metadata(qName,0,0,metacache.maxSize)
    saveMetadata(md)
  }
  
	/**
	 * delete the metadata from store and cache for a queue
   * you have to re-create the queue if you want to re-use it.
	 */
	def dropQueue(): Future[Unit] = {
		if(metacache==null) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			metadataset.delete(qName) map { case _ =>
				metacache = null
			}
		}
	}
  
	/**
	 * retrieve the size of a queue from the metadata in cache
	 */
	def getSize(): Long = {
    if(!config.localOnly)
      Await.result(readMetadata(),Duration.Inf)
		if(metacache==null) {
			throw new NotExistingQueueException(qName)
		} else {
			metacache.tail-metacache.head
		}
	}
	
	/**
	 * add a message in store for a queue
	 * update the tail counter in the metadata
	 */
	def addMessage(message:Array[Byte]): Future[Unit] = {
		if(metacache==null) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
      if(!config.localOnly)
        Await.result(readMetadata(),Duration.Inf)
			if(reachMaxSize(metacache))
				Future.failed(new ReachMaxSizeException(qName))
      else {
  			val bins : Map[String, Array[Byte]] = Map ("payload" -> message)
  			messageset.putBins(qName+"_"+(metacache.tail+1), bins).flatMap { _ => 
  				val md2 = new Metadata(metacache.qName,metacache.head,metacache.tail+1,metacache.maxSize)
  				//println("just add message on"+qName+"_"+(metacache.tail+1)+"...save metadata")
  				saveMetadataTail(md2)
  			}
      }
		}
	}
  
	/**
	 * poll a message (delete) from the store for a queue
	 * update the head counter in the metadata
	 */
	def pollMessage(): Future[Option[Array[Byte]]] = {
		if(metacache==null) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			if(emptyFifo(metacache)) {
				//println("Fifo is empty")
				Future.successful(None)
			} else {
				val md2 = new Metadata(metacache.qName,metacache.head+1,metacache.tail,metacache.maxSize)
				//println("About to poll "+qName+"_"+md2.head)
				for {
					record <- messageset.getandtouch(qName+"_"+md2.head, Option(60))
		    		res <- saveMetadataHead(md2)
				} yield record.get("payload")
			}
		}
	}
  
	/**
	 * peek a message (do not delete) from the store for a queue
	 */
	def peekMessage(): Future[Option[Array[Byte]]] = {
		if(metacache==null) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			if(emptyFifo(metacache)) {
				//println("Fifo is empty")
				Future.successful(None)
			} else {
				messageset.get(qName+"_"+(metacache.head+1), "payload")
			}
		}
	}
	
	/**
	 * delete a message from the store for a queue without read it
	 * update the head counter in the metadata
	 */
	def removeMessage(): Future[Unit] = {
		if(metacache==null) {
			Future.failed(new NotExistingQueueException(qName))
		} else {
			if(emptyFifo(metacache)) {
				//println("Fifo is empty")
				Future.failed(new EmptyQueueException(qName))
			} else {
				messageset.touch(qName+"_"+(metacache.head+1), Option(60)) flatMap { _ =>
					val md2 = new Metadata(metacache.qName,metacache.head+1,metacache.tail,metacache.maxSize)
					saveMetadataHead(md2)
				}
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
	var metrics = new MetricRegistry()
	val messagesIn = metrics.meter("messagesIn")
	
	var reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	
	var config = ConfigFactory.load().getConfig("fifo-manager")
	
	val fifo = new AeroFifo("perftest0",config)
	fifo.initialize
	
	try {
		Await.result(fifo.dropQueue(), Duration.Inf)
	} catch {
	  case t:Throwable => t.printStackTrace();
	}
	Await.result(fifo.createQueue(), Duration.Inf)
	val maxSize = 4096 -10
	val maxmessages = 200000
	val messbase = String.format("%0"+maxSize+"d", int2Integer(0))
	    
	println("E C R I T U R E")
	reporter.start(10, TimeUnit.SECONDS)
	var cpt = 0
	var f:Future[Unit] = null
	
	def recuradd() {
		f = fifo.addMessage((messbase+String.format("%010d", int2Integer(cpt))).getBytes())
		f.onSuccess { case _ =>
			cpt=cpt+1
			messagesIn.mark()
			if(cpt<maxmessages) {
				recuradd
			}
		}
	}
	recuradd
	
	while(cpt<maxmessages)
		Thread.sleep(1000)
		
	reporter.report()
	reporter.stop()
	reporter.close

	val size = fifo.getSize()
	println("size ="+size)
	if(size!=maxmessages) throw new IllegalStateException("The size is "+size)
	
	println("L E C T U R E")
	metrics = new MetricRegistry()
	val messagesOut = metrics.meter("messagesOut")
	reporter = ConsoleReporter.forRegistry(metrics)
	    	       .convertRatesTo(TimeUnit.SECONDS)
	    	       .convertDurationsTo(TimeUnit.MILLISECONDS)
	    	       .build()
	reporter.start(10, TimeUnit.SECONDS)
	cpt = 0
	def recurpoll () {
		if(fifo.getSize > 0) {
			fifo.pollMessage().onSuccess {
			  	case None => throw new IllegalStateException("None message")
				case Some(arr) => { 
				  val cur = new String(arr).toInt
				  if(cur-cpt!=0)
					  throw new IllegalStateException("Invalid counter [cur="+cur+",old="+cpt+"]")
				  cpt=cpt+1
				  messagesOut.mark()
				  recurpoll
				}
			}
		}
	}
	recurpoll
	
	while(cpt<maxmessages)
		Thread.sleep(1000)
	reporter.report()
	reporter.stop()
	reporter.close()
	
}