package com.sundogsoftware.spark.Ratings

import org.apache.log4j._
import org.apache.spark.SparkContext

object FindingTheMostPopularSuperHero extends App {


  def countItems (string : String) = {
    var words = string.split("\\s+")
    (words(0).toInt, words.length - 1)
  }

  def getHeroName (line : String) : Option[(String, String)]= {
    var words = line.split("\"")
    if (words.length == 1)
      {
        None
      }
    else
      {
        Some(words(0).trim, words(1))
      }
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Finding the Most Popular Super Hero")

  val superHeroNames = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/Marvel-names.txt")

  val superHeroesNames = superHeroNames.flatMap(getHeroName)

  //superHeroes.foreach(println)

  val superHeroGraph = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/Marvel-graph.txt")



  val superHeroMapWithCounts = superHeroGraph.map(x => countItems(x.toString))

  val superHeroTotalFriendCounts = superHeroMapWithCounts.reduceByKey((x,y) => x + y)

  val flipped = superHeroTotalFriendCounts.map(x => (x._2.toInt, x._1.toInt))

  val maxFriendsSuperHero = flipped.max()

  val output = superHeroesNames

  val popularSuperHero = superHeroesNames.lookup(maxFriendsSuperHero._2.toString)(0)

  println(s"Popular super hero is $popularSuperHero and he is appreared in ${maxFriendsSuperHero._1} times")



  val top10MostPopularHeroID = flipped.sortBy(_._1,false).take(10)
  val heroIDOnly = top10MostPopularHeroID.unzip

//  println("Hero ID only ")
  val heroIDList = heroIDOnly._2.toList

//  top10MostPopularHeroID.foreach(println)
//  superHeroesNames.top(10).foreach(println)

  heroIDList.foreach(println)


  var counter = 1
  for( c <- top10MostPopularHeroID) {
   val heroName = superHeroesNames.lookup(c._2.toString)(0)
    println(s"Hero ${heroName} appeared ${c._1} times ranks in ${counter}th place")
    counter += 1
  }

}
