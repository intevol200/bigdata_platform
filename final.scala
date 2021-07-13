// Databricks notebook source
val file = sc.textFile("/dataset/bigdata-input.txt")
val data = file.map(x => x.split('\t'))

// 1. 출발지에서 목적지로 향하는 전체 기록의 개수 세기
val depart = data.map(x => x(0))
depart.count()

// COMMAND ----------

// 2. 출발지의 ID 개수 세기
val departtinct = depart.distinct()
departtinct.count()

// COMMAND ----------

// 3. 목적지의 ID 개수 세기
val dest = data.map(x => x(1)).distinct
dest.count()

// COMMAND ----------

// 4. 출발지에서 목적지로 향하는 횟수가 가장 많은 출발지 ID와 횟수 출력
val depcount = depart.map(x => (x, 1)).reduceByKey((sum, count) => (sum + count))
depcount.takeOrdered(1)(Ordering[Int].reverse.on(x => x._2))

// COMMAND ----------

// 5. 목적지 가운데 출발지에서 가장 많이 도착된 목적지의 아이디와 횟수 출력
val descount = data.map(x => x(1)).map(x => (x, 1)).reduceByKey((sum, count) => (sum + count))
descount.takeOrdered(1)(Ordering[Int].reverse.on(x => x._2))

// COMMAND ----------

// 6. 4번과 5번 문제에서 구현한 알고리즘을 다른 방법으로 구현
val least = 10000
depart.map(x => (x, 1)).reduceByKey(_+_).filter(_._2 > least).sortBy(_._2, false).take(1)

// COMMAND ----------

// 7. HDFS에 저장하지 않고 S3에 저장된 채로 1번 문제 풀기
val s3 = sc.textFile("s3://OOOOO-us-east-1-share/bigdata-input.txt")
val s3count = s3.map(x => x.split('\t'))
s3count.map(x => x(0)).count()

// COMMAND ----------

// PageRank Algorithm
val iters = 3
val lines = sc.textFile("s3://OOOOO-us-east-1-share/bigdata-input.txt").cache()
val roads = lines.map(s => {
val splited = s.split("\t")
(splited(0), splited(1))
})
val distinctRoads = roads.distinct()
val groupedRoads = distinctRoads.groupByKey()

var results = roads.mapValues(v => 1.0)

for (i <- 1 to iters) {
val startTime = System.currentTimeMillis()
val weights = groupedRoads.join(results).values.flatMap{ case(a, b) => 
a.map(x => (x, b / a.size))
}
results = weights.reduceByKey((a, b) => a+b).mapValues(y => 0.1 + 0.9 * y)

val interimResults = results.take(10)
interimResults.foreach(record => println(record._1 + " : " + record._2))
val endTime = System.currentTimeMillis()
println(i + " th iteration took " + (endTime-startTime)/1000 + " seconds ")
}
