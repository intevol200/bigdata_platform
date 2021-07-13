// Databricks notebook source
val movies = sc.textFile("/FileStore/shared_uploads/-------@gmail.com/movies.csv")
val mfirstRow = movies.first()
val mData = movies.filter(row => row != mfirstRow)
val movieInfo = mData.map(movie => movie.split(',')).map(mi => (mi(1), mi(2).split('|')))
val actionMovies = movieInfo.filter(m => m._2.contains("Action"))

actionMovies.count()

// COMMAND ----------

val movieName = movieInfo.map(x => x._1)
val deleteYear = movieName.map(movieName => movieName.substring(0, movieName.length()-7)).distinct()

deleteYear.takeSample(false, 10)

// COMMAND ----------

val tags = sc.textFile("/FileStore/shared_uploads/-------@gmail.com/tags.csv")
val tfirstRow = tags.first()
val tData = tags.filter(row => row != tfirstRow)

val tagInfo = tData.map(tag => tag.split(',')).map(ti => ti(2))
val tagCounter = tagInfo.map(word => (word, 1)).reduceByKey((sum, count) => (sum + count))
  
tagCounter.takeOrdered(1)(Ordering[Int].reverse.on(x => x._2))

// COMMAND ----------

val ratings = sc.textFile("/FileStore/shared_uploads/-------@gmail.com/ratings.csv")
val rfirstRow = ratings.first()
val rData = ratings.filter(row => row != rfirstRow)
val rateInfo = rData.map(rating => rating.split(',')).map(ri => (ri(1), ri(2).toFloat))

// COMMAND ----------

val rateSum = rateInfo.map(rate => (rate._2))
val sum = rateSum.map(x => (x, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
sum._1 / sum._2

// COMMAND ----------

// 6번 영화별 평점을 계산하여 (movieid, 평균 평점) 만들고
// 평점이 가장 높은 100개 출력
val ratingSum = rateInfo.reduceByKey((sum, rate) => (sum + rate))
val ratingCount = rateInfo.map(rate => (rate._1)).map(id => (id, 1)).reduceByKey((sum, count) => (sum + count))

// COMMAND ----------

val joinData = ratingSum.join(ratingCount)
val ratingAvg = joinData.map(x => (x._1, x._2._1 / x._2._2))

// COMMAND ----------

ratingAvg.takeOrdered(100)(Ordering[Float].reverse.on(x => x._2))

// COMMAND ----------

// 7 1. 영화별 평점을 계산
//   2. 평점 기록 횟수 100회 이상
//   3. 상위 10개
//   4. movies와 조인한 뒤 영화아이디, 영화제목, 평점 출력

val joinC100 = ratingSum.join(ratingCount).filter(c => c._2._2 >= 100)
val c100Avg = joinC100.map(x => (x._1, x._2._1 / x._2._2))
val movieJoin = mData.map(movie => movie.split(',')).map(mi => (mi(0), mi(1)))
val top10 = movieJoin.join(c100Avg)

// COMMAND ----------

top10.takeOrdered(10)(Ordering[Float].reverse.on(x => x._2._2))
