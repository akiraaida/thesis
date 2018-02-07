import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object RWR {
  def main(args: Array[String]) {

    // Read in the sparse transition matrix (the text file is opened distributedly)
    // Split each line of the file by the "," and then match the resulting Array(i, j, val) to
    // a MatrixEntry object. Doing the proper conversion along the way
    val matrixEntries = transition.map(_ match {
      case (i, j, value) => MatrixEntry(i.toLong, j.toLong, value.toDouble)
    })

    // Construct the sparse coordinate matrix object, casting the the matrix entries to an RDD type
    val matrix = new CoordinateMatrix(matrixEntries).toIndexedRowMatrix()

    // Read in the column matrix for the multiplication
    val col = Source.fromFile("col.txt").getLines.toList.map(_.split(",").map(_.toDouble))
    // Spark matrix multiplication does not support a local sparse matrix so the column matrix
    // is dense
    var column: Matrix = Matrices.dense(col(0).size, 1, col(0))


    // Broadcast the matrix being operated on to all nodes
    sc.broadcast(column)

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
  }
}
