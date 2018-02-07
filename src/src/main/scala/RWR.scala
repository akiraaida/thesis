import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

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
    val csv = spark.read.textFile("../inc/trunc.csv")

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

    // Get only the player nodes
    val players = refMapTuple.filter(_ match {
      case (key, index) => key.contains("player")
    })

    // Take the first player as the player that will be looked for
    val targetPlayer = sc.broadcast(players.collect().head)

    // Create the column object
    val col = refMapTuple.flatMap(tuple => {
      if (tuple != targetPlayer.value) {
        Array(0.0)
      } else {
        Array(1.0)
      }
    }).collect()

    var columnMatrix = Matrices.dense(col.size, 1, col)

    // Create the transition matrix, using the key's unique id as the column and the edge's unique
    // id as the row. Calculate the value by dividing 1 (100%) by the number of edges for the key.
    val transition = mapData.flatMap(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            edges.map(edge => {
              (key, refMap.value(edge), refMap.value(key), (1.0 / edges.size * BETA))
            })
          }
        }
      }
    })

    val matrixEntries = transition.map(_ match {
      case (key, row, col, value) => MatrixEntry(row.toLong, col.toLong, value.toDouble)
    })

    val matrix = new CoordinateMatrix(matrixEntries).toIndexedRowMatrix()

    // Figure out the similarity
    val n = 10
    for(i <- 1 to n) {
      val result = matrix.multiply(columnMatrix)
      columnMatrix = result.toBlockMatrix.toLocalMatrix
    }

    // Clean up
    sc.stop()
    spark.close()
  }
}
