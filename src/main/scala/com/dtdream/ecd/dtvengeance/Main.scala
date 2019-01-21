package com.dtdream.ecd.dtvengeance

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout

import scala.concurrent.duration._
import com.dtdream.ecd.dtvengeance.actors.SparkTaskActor
import com.dtdream.ecd.dtvengeance.utils.AkkaMessages._


object Main extends App {
  val system = ActorSystem("akka_etl_system")
  implicit val timeout = Timeout(5 seconds)

  val taskRef = system.actorOf(Props[SparkTaskActor],"task_test_actor")

  if (args.length > 0) {
    taskRef ! TaskMessage(args(0))

  }

  system.terminate()
}
