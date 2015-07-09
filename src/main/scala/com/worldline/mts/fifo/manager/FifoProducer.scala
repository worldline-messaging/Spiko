package com.worldline.mts.fifo.manager

import scala.concurrent.Await
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object FifoProducer extends App {
  var metrics = new MetricRegistry()
  val messagesIn = metrics.meter("messagesIn")
  
  var reporter = ConsoleReporter.forRegistry(metrics)
               .convertRatesTo(TimeUnit.SECONDS)
               .convertDurationsTo(TimeUnit.MILLISECONDS)
               .build()
  
  var config = ConfigFactory.load().getConfig("fifo-manager")
  
  val fifo = new AeroFifo("perftest0",config)
  fifo.initialize
  
  val maxSize = 1024 -10
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
}