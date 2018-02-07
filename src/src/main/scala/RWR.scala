import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object PrepData {
  def main(args: Array[String]) {

    // Constants
    val PING_BUCKET_SIZE = 25
    val MMR_BUCKET_SIZE = 1

    // Initialization
    val spark = SparkSession.builder().master("local").appName("RWR").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Load the csv file in
    val csv = spark.read.textFile("../../inc/data.csv")

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
    }).groupByKey.zipWithIndex

    // Map each node to a unique integer (the index in this case) to differentiate nodes
    val refMapTuple = mapData.map(_ match {
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
    val refMap = sc.broadcast(refMapTuple.collectAsMap)

    // Create the transition matrix, using the key's unique id as the column and the edge's unique
    // id as the row. Calculate the value by dividing 1 (100%) by the number of edges for the key.
    val transition = mapData.flatMap(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            edges.map(edge => (key, refMap.value(edge), refMap.value(key), (1.0 / edges.size)))
          }
        }
      }
    })

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



    /*
    // Create csv values for each matrix entry
    val saveData = transition.map(_ match {
      case (key, row, col, value) => {
        key + "," + row + "," + col + "," + value
      }
    })

    // Save the matrix to sparse
    saveData.saveAsTextFile("transition")
    */

    // Clean up
    sc.stop()
    spark.close()
  }
}
