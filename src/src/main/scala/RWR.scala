import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object RWR {
  def main(args: Array[String]) {

    // Constants
    val PING_BUCKET_SIZE = 25
    val MMR_BUCKET_SIZE = 1
    val BETA = 0.8
    val TOP = 1
    val MASTER = "local"
    val INPUT_FILE = "../inc/data.csv"

    // Initialization
    val spark = SparkSession.builder().master(MASTER).appName("RWR").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Load the csv file into Spark
    val csv = sc.textFile(INPUT_FILE)

    // Split each entry on the comma getting an object of Array entries, an Array entry representing
    // a player
    val data = csv.map(_.split(","))

    // Filter the array of data down to the needed attributes. This results in an object of
    // (leagueIndex, actionLatency, index) entries.
    val filterData = data.map(_ match {
      case Array(gameId, leagueIndex, age, hoursPerWeek, totalHours, apm, selectByHotKeys,
          assignToHotKeys, uniqueHotKeys, minimapAttacks, minimapRightClicks, numberOfPacs,
          gapBetweenPacs, actionLatency, actionsInPac, totalMapExplored, workersMade,
          uniqueUnitsMade, complexUnitsMade, complexAbilitiesUsed, priority) =>
          (leagueIndex.toDouble, actionLatency.toDouble, priority.toInt)
    })

    // The filtered data is then constructed into meaningful node values where each entry of the
    // filtered data will result in the creation of 4 nodes in an Array.
    // Ping (bucket) -> Player (unique index)
    // Player (unique index) -> Ping (bucket)
    // MMR (bucket) -> Player (unique index)
    // Player (unique index) -> MMR (bucket)
    // After 4 nodes have been created for each filtered data result, the nodes are aggregated
    // to key -> values which is effectively the node and edges. ie. player0 -> (mmr1, ping3).
    // This resulting data structure is then given an index for each entry.
    val mapData = filterData.flatMap(_ match {
      case (mmr, ping, priority) => {
        Array(
          ("ping" + Math.round(ping / PING_BUCKET_SIZE), "player" + priority),
          ("player" + priority, "ping" + Math.round(ping / PING_BUCKET_SIZE)),
          ("mmr" + Math.round(mmr / MMR_BUCKET_SIZE), "player" + priority),
          ("player" + priority, "mmr" + Math.round(mmr / MMR_BUCKET_SIZE))
        )
      }
    }).groupByKey.zipWithIndex

    // Map each node -> edges entry to a node -> index entry to differentiate each node.
    val refMapTuple = mapData.map(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            (key, index)
          }
        }
      }
    })

    // Collect the node -> index map as a hashmap onto the driver node and broadcast it to all
    // nodes. This hashmap will be used to lookup the row/columns when building the transition
    // matrix.
    val refMap = sc.broadcast(refMapTuple.collectAsMap)

    // Get the player with the lowest priority (0)
    val targetIndex = sc.broadcast(filterData.filter(_ match {
      case (mmr, ping, priority) => {
        priority == 0
      }
    }).map(_ match {
      case (mmr, ping, priority) => {
        priority
      }
    }).first())

    // Using the line index, find the player that has the same index by filtering out every other
    // value then mapping it as a key with the unordered index (used for the transition matrix).
    // Afterwards, make this value a broadcast variable
    val targetPlayer = sc.broadcast(mapData.filter(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            key == ("player" + targetIndex.value)
          }
        }
      }
    }).map(_ match {
      case ((key, edges), index) => {
        (key, index)
      }
    }).first())

    // Clean up the broadcast variable since it will not be used again past this point.
    // This will save some memory on the distributed nodes.
    targetIndex.unpersist(blocking = true)

    // Construct the column matrix which is a value of 1 where the target player's row is and
    // a 0 everywhere else. The 0 entries are then filtered out to to save space in memory.
    var columnMatrix = refMapTuple.map(_ match {
      case (data, index) => {
        if (index == targetPlayer.value._2) {
          (index, (0.toLong, 1.toDouble))
        } else {
          (index, (0.toLong, 0.toDouble))
        }
      }
    }).filter(_ match {
      case (row, (col, value)) => value > 0
    }) 

    // Calculate the (1 - B) * e_N portion. This will result in a data structure of 
    // ((row, col), val) which will be added to the transition matrix.
    val eN = refMapTuple.map(_ match {
      case (data, index) => {
        ((targetPlayer.value._2, index), 1.0 - BETA)
      }
    })

    // Create the transition matrix by creating a probability value for each edge of a given node.
    // This is done by taking each edge of the node and dividing 1 (for 100%) by the number of
    // edges. This value is then multiplied by BETA which is the chance the random walker will
    // continue walking at random (instead of teleporting back to the start). This value is mapped
    // to a specific row and column based on the unique value for the key and edge. After this
    // is completed, the (1 - B) * e_N portion is added to the transition matrix and then the
    // entire transition matrix is formatted to (col, (row, value)) for later matrix multiplication.
    val transition = mapData.flatMap(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            edges.map(edge => {
              ((refMap.value(edge), refMap.value(key)), (1.0 / edges.size) * BETA)
            })
          }
        }
      }
    }).union(eN).reduceByKey(_ + _).map(_ match {
      case ((row, col), value) => (col, (row, value))
    }).cache()

    // Clean up the broadcast variable since it will not be used again past this point.
    // This will save some memory on the distributed nodes.
    refMap.unpersist(blocking = true)

    // Define a function to do the matrix multiplication. The function will take the transition
    // matrix and the column matrix. The assumption for these data structures is that they'll
    // be in the format of...
    // Left Matrix (Transition) = (col, (row, value))
    // Right Matrix (Column) = (row, (col, value))
    // So that they can be joined on the row/col key to do the multiplication. Then the result
    // of this transformation will be to generate ((row, col), value) entries for each
    // multiplication. This can then be aggregated by using the (row, col) as a key and doing
    // the addition portion of the matrix multiplication. Afterwards the entries are converted
    // back into (row, (col, value)) entries to make the next iterations calculation easier.
    def matrixMultiply(transitionMatrix:org.apache.spark.rdd.RDD[(Long, (Long, Double))],
                       columnMatrix:org.apache.spark.rdd.RDD[(Long, (Long, Double))]):
                       org.apache.spark.rdd.RDD[(Long, (Long, Double))] = {

      val newMatrix = transition.join(columnMatrix).map(_ match {
        case (key, ((row, value1), (col, value2))) => ((row, col), value1*value2)
      }).reduceByKey(_ + _).map(_ match {
        case ((row, col), value) => (row, (col, value))
      })

      newMatrix
    }

    // Continuously multiply the transition matrix by each permutation of the column matrix.
    // This will result in the similiarity values for the target player vs. the rest of the nodes.
    val iter1 = matrixMultiply(transition, columnMatrix)
    val iter2 = matrixMultiply(transition, iter1)
    val iter3 = matrixMultiply(transition, iter2)
    val iter4 = matrixMultiply(transition, iter3)
    val iter5 = matrixMultiply(transition, iter4)
    val iter6 = matrixMultiply(transition, iter5)
    val iter7 = matrixMultiply(transition, iter6)
    val iter8 = matrixMultiply(transition, iter7)
    val iter9 = matrixMultiply(transition, iter8)
    val result = matrixMultiply(transition, iter9)

    // Create a broadcast variable to map the unique value assigned to each node back to the name
    // of the node. Only the player nodes are looked at for this data structure so the rest of the
    // nodes are filtered out.
    val playerMap = sc.broadcast(refMapTuple.filter(_ match {
      case (key, index) => key.contains("player")
    }).map(_ match {
      case (key, index) => (index, key)
    }).collectAsMap)

    // Filter out the values in the similarity matches that are not players and are not the target
    // player. Then take the TOP number of highest similarity values.
    val matches = result.filter(_ match {
      case (row, (col, sim)) => playerMap.value.get(row) != None
    }).filter(_ match {
      case (row, (col, sim)) => row != targetPlayer.value._2
    }).map(_ match {
      case (row, (col, sim)) => (playerMap.value(row), sim)
    }).takeOrdered(TOP)(Ordering[Double].reverse.on(_._2))

    // Print the matches
    println(targetPlayer.value)
    matches.map(println(_))

    // Clean up the broadcast variable since they will not be used any longer.
    playerMap.unpersist(blocking = true)
    targetPlayer.unpersist(blocking = true)

    // Clean up
    sc.stop()
    spark.close()
  }
}
