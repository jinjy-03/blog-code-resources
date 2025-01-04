import scala.collection.mutable.PriorityQueue

val startTime = System.currentTimeMillis()

// Step 1: 统计字符频率
val rdd = sc.textFile("hdfs://namenode:8020/input/AFR.txt")
val charCounts = rdd.flatMap(line => line.toCharArray)
  .filter(_.isLetterOrDigit)
  .map(char => (char, 1))
  .reduceByKey(_ + _)
  .collect()

// Step 2: 定义哈夫曼树节点
case class Node(char: Option[Char], freq: Int, left: Option[Node], right: Option[Node])

// 构建哈夫曼树
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

// Step 3: 生成哈夫曼编码
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

// Step 4: 使用哈夫曼编码替换文件内容
val encodedFileRdd = rdd.map(line => line.flatMap(char => huffmanCodes.getOrElse(char, "")))

// Step 5: 保存哈夫曼编码文件到 HDFS
encodedFileRdd.saveAsTextFile("hdfs://namenode:8020/output/AFR_file_encoded.txt")

val endTime = System.currentTimeMillis()
println(s"Total execution time: ${(endTime - startTime) / 1000.0} seconds")