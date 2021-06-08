//package org.apache.flink.quickstart
//
//import org.apache.flink.Types.OrganizationClosureDate
//import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
//
//import java.time.LocalDate
//import scala.util.Random
//
//class OrganizationClosureDateSource(initialDelay: Long, delay: Long) extends RichSourceFunction[OrganizationClosureDate] {
//
//  val random = new Random()
//
//  var running = true
//
//  var count = 0
//
//  override def run(sourceContext: SourceFunction.SourceContext[OrganizationClosureDate]): Unit = {
//    Thread.sleep(initialDelay)
//    while (running) {
//      if (count % 10 == 0) {
//        organizations
//          //        .filter(organization => !organization.equals(organizations.get(random.nextInt(organizations.size()))))
//          .map(organization => OrganizationClosureDate(organization, None))
//          .foreach(tuple => sourceContext.collect(tuple))
//      } else {
//        organizations
//          //        .filter(organization => !organization.equals(organizations.get(random.nextInt(organizations.size()))))
//          .map(organization => OrganizationClosureDate(organization, Some(LocalDate.now())))
//          .foreach(tuple => sourceContext.collect(tuple))
//      }
//
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
