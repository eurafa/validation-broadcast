//package org.apache.flink.quickstart
//
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
//
//class EventSource(delay: Long) extends RichParallelSourceFunction[Event] {
//
//  var running = true
//  var count = 1
//
//  //  val organizations = List[String]()
//
//
//  override def open(parameters: Configuration): Unit = {
//    System.out.println("-".repeat(40))
//    //    organizations.add("0101")
//    //    organizations.add("0116")
//    //    organizations.add("0812")
//  }
//
//  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
//    while (running) {
//      organizations
//        .foreach(organization => sourceContext.collect(
//          Event(organization, count)))
//      count = count + 1
//      Thread.sleep(delay)
//    }
//  }
//
//  override def cancel(): Unit = {
//    this.running = false
//  }
//
//}
