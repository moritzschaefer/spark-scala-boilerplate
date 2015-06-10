import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // for implicit conversion


// Example code to count words.
object Main {
  def main(args: Array[String]) {

    val inputFile = "/tmp/infile"
    val outputFile = "/tmp/outdir"

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split it up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a
    counts.saveAsTextFile(outputFile)

  }
}
