package com.AmaxonReviews


import scala.concurrent.{ Await, Future }
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt


// To run on AWS EMR successfully
// Alternatively it could have been run on a EC2 compute cluster as well
// spark-submit --executor-memory 1g ReviewSimilarities1M.jar 260


object ReviewSimilarities1M {
  
  /** Load up a Map of Review IDs to Review names. */
  def loadReviewNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var ReviewNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("Reviews.dat").getLines()
     for (line <- lines) {
       var fields = line.split("::")
       if (fields.length > 1) {
        ReviewNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return ReviewNames
  }
  
  type ReviewRating = (Int, Double)
  type UserRatingPair = (Int, (ReviewRating, ReviewRating))
  def makePairs(userRatings:UserRatingPair) = {
    val ReviewRating1 = userRatings._2._1
    val ReviewRating2 = userRatings._2._2
    
    val Review1 = ReviewRating1._1
    val rating1 = ReviewRating1._2
    val Review2 = ReviewRating2._1
    val rating2 = ReviewRating2._2
    
    ((Review1, Review2), (rating1, rating2))
  }
  
  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val ReviewRating1 = userRatings._2._1
    val ReviewRating2 = userRatings._2._2
    
    val Review1 = ReviewRating1._1
    val Review2 = ReviewRating2._1
    
    return Review1 < Review2
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext without much actual configuration
    // We want EMR's config defaults to be used.
    val conf = new SparkConf()
    conf.setAppName("ReviewSimilarities1M")
    val sc = new SparkContext(conf)
    
    println("\nLoading Review names...")
    val nameDict = loadReviewNames()
 // I have omitted the file path for company privacy reasons   
    val data = sc.textFile("s3n://.dat")

    // Map ratings to key / value pairs: user ID => Review ID, rating
    val ratings = data.map(l => l.split("::")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
    
    // Emit every Review rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)   
    
    // At this point our RDD consists of userID => ((ReviewID, rating), (ReviewID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (Review1, Review2) pairs.
    val ReviewPairs = uniqueJoinedRatings.map(makePairs).partitionBy(new HashPartitioner(100))

    // We now have (Review1, Review2) => (rating1, rating2)
    // Now collect all ratings for each Review pair and compute similarity
    val ReviewPairRatings = ReviewPairs.groupByKey()

    // We now have (Review1, Review2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val ReviewPairSimilarities = ReviewPairRatings.mapValues(computeCosineSimilarity).cache()
    
    //Save the results if desired
    //val sorted = ReviewPairSimilarities.sortByKey()
    //sorted.saveAsTextFile("Review-sims")
    
    // Extract similarities for the Review we care about that are "good".
    
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 1000.0
      
      val ReviewID:Int = args(0).toInt
      
      // Filter for Reviews with this sim that are "good" as defined by
      // our quality thresholds above     
      
      val filteredResults = ReviewPairSimilarities.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == ReviewID || pair._2 == ReviewID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(50)
      
      println("\nTop 50 similar Reviews for " + nameDict(ReviewID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the Review we're looking at
        var similarReviewID = pair._1
        if (similarReviewID == ReviewID) {
          similarReviewID = pair._2
        }
        println(nameDict(similarReviewID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
}

