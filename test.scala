// Databricks notebook source
def square(x: Double) : Double = x * x
val x = 9
square(x)

var y = 10
square(y)
y = 11

// COMMAND ----------

val x = 0
def f(y: Int) = y + 1

val result = {
  val x = f(3)
  x * x
} + x

// COMMAND ----------

class ChecksumAccumulator {
  var publicSum = 0
  private var sum = 0
  
  def add(b: Byte): Unit = {
    sum += b
  }
  
  def checksum(): Int = {
    return ~(sum &0xFF) + 1
  }
}

// COMMAND ----------

var myClass = new ChecksumAccumulator()
myClass.publicSum
myClass.checksum

// COMMAND ----------

class Person(var firstName:String, val lastName:String, age: Int)

// COMMAND ----------

var message = "hello scala"

// COMMAND ----------

val movies = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv")
println(movies.count())
println(movies.first())
movies.take(2)

// COMMAND ----------

val action_movies = movies.filter(movie => movie.contains("action"))
action_movies.count()

// COMMAND ----------

val movies = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv")
val movieInfo = movies.map(movie => movie.split(','))
val mi = movieInfo.take(2)
val m = movies.take(2)

// COMMAND ----------

val genres = movieInfo.map(mi => mi(2).split("|"))
val mapGenre = genres.take(2)
val flatMapGenre = movieInfo.flatMap(mi => mi(2).split("|").take(2))

// COMMAND ----------

val movieGenres = movieInfo.map(mi => (mi(1), mi(2).split('|')))
val actionMovies = movieGenres.filter(m => m._2.contains("Action"))
actionMovies.take(1)
actionMovies.count()

// COMMAND ----------

val rdd1 = sc.parallelize(List("coffee", "coffee", "panda", "monkey", "tea"))
val rdd2 = sc.parallelize(List("coffee", "monkey", "kitty"))
rdd1.distinct()
rdd1.distinct().collect()
rdd1.union(rdd2).collect()
rdd1.union(rdd2).distinct().collect()
rdd1.intersection(rdd2).collect()
rdd1.subtract(rdd2).collect()

// COMMAND ----------

val input = sc.parallelize(List(1,2,3,4,5))
input.reduce((a,b) => a+b)

// COMMAND ----------

movieInfo.map(mi => mi(2).split('|')).map(x => x.length).reduce((x,y) => Math.max(x,y))

// COMMAND ----------

sc.parallelize(List(1,2,3,4,5)).collect()
sc.parallelize(List(1,2,3,4,5)).take(2)
sc.parallelize(List(1,2,3,4,5)).takeOrdered(2)(Ordering[Int].reverse)

// COMMAND ----------

movieInfo.takeSample(false, 5)
movieInfo.count()
sc.parallelize(List(1,2,3,2,4,2,1,4,3,5)).countByValue()

// COMMAND ----------

// movieInfo.map(x => x(1)).saveAsTextFile("/FileStore/shared_uploads/intevol200@gmail.com/outputs")
display(dbutils.fs.ls("/FileStore/shared_uploads/intevol200@gmail.com/outputs"))

// COMMAND ----------

val movies = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv")
println(movies.partitions.length)
val moviesRepart = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv", 10)
println(moviesRepart.partitions.length)
val repartitionedMovies = moviesRepart.repartition(4)
println(repartitionedMovies.partitions.length)

// COMMAND ----------

val input = sc.parallelize(List("dog 1", "cat 1", "dog 1", "dog 1", "cat 1"))
val pairs = input.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))
pairs.collect

// COMMAND ----------

val gbk = pairs.groupByKey().collect()
val gbv = pairs.reduceByKey((x,y) => (x+y)).collect()
val mv = pairs.mapValues(x => x+1).collect()
val kc = pairs.keys.collect
val vc = pairs.values.collect
val sbk = pairs.sortByKey().collect

// COMMAND ----------

val rdd1 = sc.makeRDD(Array(("A","1"),("A","4"),("B","2"),("C","3")))
val rdd2 = sc.makeRDD(Array(("A","a"),("A","a"),("C","c"),("D","d")))
val sbk = rdd1.subtractByKey(rdd2).collect
val j = rdd1.join(rdd2).collect
val cg = rdd1.cogroup(rdd2).collect

// COMMAND ----------

val movies = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv")
val movieInfo = movies.map(movie => movie.split(','))
val genres = movieInfo.flatMap(mi => mi(2).split('|'))
val genreOne = genres.map(g => (g, 1))
val genreCounter = genreOne.reduceByKey((x,y) => (x+y))
genreCounter.takeOrdered(10)(Ordering[Int].reverse.on(x => x._2))

// COMMAND ----------

val NUM_SAMPLES = 100000
val exec = sc.parallelize(1 to NUM_SAMPLES)
val inCircle = exec.map(i => { val x = Math.random()
                             val y = Math.random()
                             if (x*x + y*y < 1.0) 1 else 0})
val count = inCircle.reduce((v1, v2) => v1 + v2)
println("Estimates Pi is " + 4.0*count/NUM_SAMPLES)

// COMMAND ----------

val stop_words = sc.broadcast(List("is","a"))
val input = sc.parallelize(List("This is a dog", "This is a cat"))
val cbv = input.flatMap(line => line.split(" ")).countByValue()
val swCbv = input.flatMap(line => line.split(" ")).filter(x => !stop_words.value.contains(x)).countByValue()

// COMMAND ----------

import org.apache.spark.storage.StorageLevel
val moviesPersist = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv").persist(StorageLevel.MEMORY_ONLY)
val moviesCache = sc.textFile("/FileStore/shared_uploads/intevol200@gmail.com/movies.csv").cache()
