package com.quantum.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalCustomerOrder {
  
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val customerId = fields(0).toInt
      val amount = fields(2).toFloat
      // Create a tuple that is our result.
      (customerId, amount)
  }

  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)        
    
    val sc = new SparkContext("local[*]", "FriendsByAge")
    
    val lines = sc.textFile("../customer-orders.csv")
    
    val rdd = lines.map(parseLine)
    
    val total = rdd.reduceByKey((x,y) => x + y).collect().sorted.foreach(println)
    
    
  }
  
  
}