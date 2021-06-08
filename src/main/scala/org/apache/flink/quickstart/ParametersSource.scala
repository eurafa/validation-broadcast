//package org.apache.flink.quickstart
//
//import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
//
//import scala.util.Random
//
//class ParametersSource(initialDelay: Long, delay: Long) extends RichSourceFunction[EventParameters] {
//
//  val random = new Random()
//
//  var running = true
//
//  var organizationToggle = false
//
//  override def run(sourceContext: SourceFunction.SourceContext[EventParameters]): Unit = {
//    Thread.sleep(initialDelay)
//    while (running) {
//      var organization = ""
//      if (organizationToggle) {
//        organization = "0116"
//      } else {
//        organization = "0101"
//      }
//      organizationToggle = !organizationToggle
//
//      val limits = Seq(EventParameterRule(random.nextInt(10) +1))
//
//      sourceContext.collect(EventParameters(organization, limits))
//
//      Thread.sleep(delay)
//    }
//  }
//
//  override def cancel(): Unit = {
//    this.running = false
//  }
//
//}
