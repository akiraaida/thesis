import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object RWR {
  def main(args: Array[String]) {

    // Initialization
    val spark = SparkSession.builder().master("local").appName("RWR").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Load the csv file in
    val csv = spark.read.textFile("../../inc/transition.csv")

    // Read in the sparse transition matrix (the text file is opened distributedly)
    // Split each line of the file by the "," and then match the resulting values to
    // a MatrixEntry object. Doing the proper conversion along the way
    val matrixEntries = csv.map(_.split(",") match {
      case Array(key, row, col, value) => MatrixEntry(row.toLong, col.toLong, value.toDouble)
    })

    // Construct the sparse coordinate matrix object
    val matrix = new CoordinateMatrix(matrixEntries.rdd).toIndexedRowMatrix()

    /*
    // Figure out the similarity
    val n = 10
    for(i <- 1 to n) {
      val result = matrix.multiply(column)
      column = result.toBlockMatrix.toLocalMatrix

      if (i == n) {
        val output = result.rows.map(_.vector.toDense)
        output.coalesce(1, true).saveAsTextFile("result")
      }
    }
    */
  }
}
