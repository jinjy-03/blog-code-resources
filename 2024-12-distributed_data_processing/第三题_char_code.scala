import scala.collection.mutable.PriorityQueue

val startTime = System.currentTimeMillis()

val rdd = sc.textFile("hdfs://namenode:8020/input/AFR.txt")
val charCounts = rdd.flatMap(line => line.toCharArray)
  .filter(_.isLetterOrDigit)
  .map(char => (char, 1))
  .reduceByKey(_ + _)
  .collect()

case class Node(char: Option[Char], freq: Int, left: Option[Node], right: Option[Node])


val pq = PriorityQueue.empty[Node](Ordering.by(-_.freq)) 
charCounts.foreach { case (char, freq) =>
  pq.enqueue(Node(Some(char), freq, None, None))
}

while (pq.size > 1) {
  val left = pq.dequeue()
  val right = pq.dequeue()
  pq.enqueue(Node(None, left.freq + right.freq, Some(left), Some(right)))
}

val huffmanTree = pq.dequeue()


def generateCodes(node: Option[Node], prefix: String, codes: collection.mutable.Map[Char, String]): Unit = {
  node match {
    case Some(Node(Some(char), _, None, None)) => codes += (char -> prefix) 
    case Some(Node(_, _, left, right)) =>
      generateCodes(left, prefix + "0", codes)
      generateCodes(right, prefix + "1", codes)
    case _ => 
  }
}

val huffmanCodes = collection.mutable.Map[Char, String]()
generateCodes(Some(huffmanTree), "", huffmanCodes)

val huffmanRdd = sc.parallelize(huffmanCodes.toSeq).map { case (char, code) => s"$char -> $code" }
huffmanRdd.saveAsTextFile("hdfs://namenode:8020/output/AFR_char_code.txt")

val endTime = System.currentTimeMillis()
println(s"Total execution time: ${(endTime - startTime) / 1000.0} seconds")