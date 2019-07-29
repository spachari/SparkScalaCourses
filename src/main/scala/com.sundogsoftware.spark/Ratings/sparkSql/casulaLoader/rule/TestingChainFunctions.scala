package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.rule

class TestingChainFunctions(var a : Int) {

  def addAndSubtract(): Unit = {
    add(10).sub(5)
  }

  def add(i : Int) = {
    this.a = this.a + i
    println("Inside add method " + this.a)
    this
  }

  def sub(i : Int) = {
    this.a = this.a - i
    println("inside sub method " + this.a)
    this
  }

  override def toString: String = a.toString
}


class TestingChainFunctionsOverList(var list : List[Int]) {

  def add() = {
    this.list = list.map(x => x + 100)
    this
  }

  def sub() = {
    this.list = list.map(x => x + 10)
    this
  }

  override def toString: String = list.mkString(", ")
}

object Test extends App {
  val t = new TestingChainFunctions(100)

  t.addAndSubtract()

  println(t)

  val a : Int => Int = (a : Int) => a * 10

  t.add(10)

  println(t)

  val list = List(100,200,300,400,500)

  val output = list.map {
    x =>
    val t = new TestingChainFunctions(x)
    t.addAndSubtract()
    t
  }

  output.foreach(println)

  val chainEx = new TestingChainFunctionsOverList(list)

  val output1 = chainEx.add().sub()

  chainEx.list.foreach(println)
}
