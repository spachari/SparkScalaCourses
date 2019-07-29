package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

case class Person (name : String, age : Int) extends Ordered[Person] {

  override def compare(that: Person) = {
    if (this.age > that.age)
      this.age
    else
      that.age
  }

}


object AggreagateOnPerson extends App {

  type score = (Int, Double)

  def printScore (score : score) = {
    val (int, double) = score
    println(int)
    println(double)
  }


  val one = (10,10.0)
  printScore(one)


  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using the local machine
  val sc = new SparkContext("local", "PersonAgemaximum")

  case class Person(name : String, age: Int)

  val ben =  Person("Ben", 6)
  val matt =  Person("Matt", 37)
  val steve =  Person("Steve", 27)
  val don =  Person("Don", 17)
  val pete =  Person("Pete", 16)

  def myMax( a : Person, b : Person) : Person = {
    if (a.age >= b.age)
      a
    else
      b
  }

  def myFirst(a : Person, b : Person) : Person = {
    a
  }

  val friends = sc.parallelize(List(matt,steve,don,pete)).persist(StorageLevel.MEMORY_AND_DISK)

  val maxAge = friends.map(x => x.age).reduce((x,y) => x max y)

  println(maxAge)

  val outputMax = friends.reduce((x,y) => myMax(x,y))

  println("Output for person " + outputMax.age + " " + outputMax.name)

  val outputFirst = friends.reduce((x,y) => myFirst(x,y))

  println("Output for person " + outputFirst.age + " " + outputFirst.name)


  //Pairs of RDDs
  val benMath =  Person("Ben", 45)
  val benSci =  Person("Ben", 90)
  val mattMath =  Person("Matt", 67)
  val steveMath =  Person("Matt", 77)


  val friendMarks = sc.parallelize(List(benMath,benSci,mattMath,steveMath)).persist(StorageLevel.MEMORY_AND_DISK)

  val pairRDD = friendMarks.map(x => (x.name, x.age)).combineByKey(
    (v) => (v,1),
    (acc: (Int,Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1 : (Int, Int), acc2 : (Int, Int)) => (acc1._1 + acc2._1, acc2._1 + acc2._2)
  ).map { case (key, value) => (key, value._1 / value._2)}

  pairRDD.foreach(println)

}
