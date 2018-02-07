import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object RWR {
  def main(args: Array[String]) {

    // Constants
    val PING_BUCKET_SIZE = 25
    val MMR_BUCKET_SIZE = 1
    val BETA = 0.8

    // Initialization
    val spark = SparkSession.builder().master("local").appName("RWR").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Load the csv file in
    val csv = spark.read.textFile("trunc.csv")

    // Filter the data to what is needed, converting it to an RDD and adding the index
    val filterData = csv.map(_.split(",") match {
      case Array(gameId, leagueIndex, age, hoursPerWeek, totalHours, apm, selectByHotKeys,
        assignToHotKeys, uniqueHotKeys, minimapAttacks, minimapRightClicks, numberOfPacs,
        gapBetweenPacs, actionLatency, actionsInPac, totalMapExplored, workersMade,
        uniqueUnitsMade, complexUnitsMade, complexAbilitiesUsed) => Array(leagueIndex.toDouble,
        actionLatency.toDouble)
    }).rdd.zipWithIndex

    // Assign edges to all of the nodes in the data set
    val mapData = filterData.flatMap(_ match {
      case (data, index) => {
        data match {
          case Array(mmr, ping) => {
            Array(
              ("ping" + Math.round(ping / PING_BUCKET_SIZE), "player" + index),
              ("player" + index, "ping" + Math.round(ping / PING_BUCKET_SIZE)),
              ("mmr" + Math.round(mmr / MMR_BUCKET_SIZE), "player" + index),
              ("player" + index, "mmr" + Math.round(mmr / MMR_BUCKET_SIZE))
            )
          }
        }
      }
    }).groupByKey

    // Map each node to a unique integer (the index in this case) to differentiate nodes
    val refMapTuple = mapData.zipWithIndex.map(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            (key, index)
          }
        }
      }
    })
    
    // Collect the tuple map onto the driver node, convert it into a key -> val hashmap. Then
    // broadcast the reference map to each node. The collect operation is needed to convert the data
    // structure into a map from an RDD tuple. The size of the map should be relatively small
    // so this should not be an issue memory wise for the driver node.
    val refMap  = refMapTuple.collect().toMap
    sc.broadcast(refMap)

    // Create the transition matrix, using the key's unique id as the column and the edge's unique
    // id as the row. Calculate the value by dividing 1 (100%) by the number of edges for the key.
    // Then each value is multiplied by the beta value (the chance the walker will continue
    // randomly walking)
    val transition = mapData.flatMap(_ match {
      case (key, edges) => {
        edges.map(edge => (refMap(edge), refMap(key), (1.0 / edges.size) * BETA))
      }
    })

    // Read in the sparse transition matrix (the text file is opened distributedly)
    // Split each line of the file by the "," and then match the resulting Array(i, j, val) to
    // a MatrixEntry object. Doing the proper conversion along the way
    val matrixEntries = transition.map(_ match {
      case (i, j, value) => MatrixEntry(i.toLong, j.toLong, value.toDouble)
    })

    // Construct the sparse coordinate matrix object, casting the the matrix entries to an RDD type
    val matrix = new CoordinateMatrix(matrixEntries).toIndexedRowMatrix()

    /*
    // Read in the column matrix for the multiplication
    val col = Source.fromFile("col.txt").getLines.toList.map(_.split(",").map(_.toDouble))
    // Spark matrix multiplication does not support a local sparse matrix so the column matrix
    // is dense
    var column: Matrix = Matrices.dense(col(0).size, 1, col(0))


    // Broadcast the matrix being operated on to all nodes
    sc.broadcast(column)
    */
    

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
