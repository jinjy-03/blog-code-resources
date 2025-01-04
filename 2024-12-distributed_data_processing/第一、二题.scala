
// 统计每个字符的频

val startTime = System.currentTimeMillis() 
val rdd = sc.textFile("hdfs://namenode:8020/input/MR.txt")
val charCounts = rdd.flatMap(line => line.toCharArray).filter(_.isLetterOrDigit).map(char => (char, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = true)
charCounts.saveAsTextFile("hdfs://namenode:8020/output/char.txt")
val endTime = System.currentTimeMillis() 
println(s"Total execution time: ${(endTime - startTime) / 1000.0} seconds")

// 按照升序输出文件中每个单词出现的频率到文件（word.txt）
val startTime = System.currentTimeMillis()

val wordCounts = sc.textFile("hdfs://namenode:8020/input/MR.txt").map(line => line.replaceAll("<[^>]+>", "")).flatMap(line => line.split("\\s+")).map(word => word.replaceAll("^[^a-zA-Z'-]+|[^a-zA-Z'-]+$", "")).filter(word => word.matches("^[a-zA-Z]+(?:['-][a-zA-Z]+)*$")).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = true)

wordCounts.saveAsTextFile("hdfs://namenode:8020/output/word.txt")

val endTime = System.currentTimeMillis()
println(s"Total execution time: ${(endTime - startTime) / 1000.0} seconds")