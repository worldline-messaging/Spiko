package com.worldline.mts.fifo.manager

import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit
import com.codahale.metrics.ConsoleReporter
import scala.concurrent.Await
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object FifoConsumer extends App {
  var metrics = new MetricRegistry()
  var config = ConfigFactory.load().getConfig("fifo-manager")
  
  val fifo = new AeroFifo("perftest0",config)
  fifo.initialize
  
  println("L E C T U R E")
  val messagesOut = metrics.meter("messagesOut")
  val reporter = ConsoleReporter.forRegistry(metrics)
               .convertRatesTo(TimeUnit.SECONDS)
               .convertDurationsTo(TimeUnit.MILLISECONDS)
               .build()
  reporter.start(10, TimeUnit.SECONDS)
  var cpt = 0
  def recurpoll () { 
    while(fifo.getSize==0 && cpt<200000) {
      println("NO MESSAGE")
      Thread.sleep(1000)  
    }
    if(fifo.getSize > 0 && cpt<200000) {
      //println("THERE'S MESSAGES:"+size+" CPT="+cpt)
      val fpoll = fifo.pollMessage()
      fpoll.onSuccess {
        case None => throw new IllegalStateException("None message")
        case Some(arr) => { 
          val cur = new String(arr).toInt
          if(cur-cpt!=0)
            println("INVALID COUNTER [cur="+cur+",old="+cpt+"]")
          cpt=cpt+1
          messagesOut.mark()
          recurpoll
        }
      }
      fpoll.onFailure {
        case e => e.printStackTrace()
      }
    }
  }
  recurpoll
  
  while(cpt<200000) {
    Thread.sleep(1000)  
  }
  val size = fifo.getSize()
  if(size!= 0) throw new IllegalStateException("The size is "+size)
  
  reporter.report()
  reporter.stop()
  reporter.close()
}