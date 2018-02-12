import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object RWR {
  def main(args: Array[String]) {

    // Constants
    val PING_BUCKET_SIZE = 1
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

    // Assign edges to all of the nodes in the data set. Assigning the index at this point
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

    // Collect the tuple map as a hashmap onto the driver node and broadcast it to all nodes.
    // This will be used to lookup the row/columns and vice versa
    val refMap = sc.broadcast(refMapTuple.collectAsMap)

    // Get only the player nodes
    val players = refMapTuple.filter(_ match {
      case (key, index) => key.contains("player")
    })

    // Take the first player. This will be the player that we will find the similarity for
    val targetPlayer = sc.broadcast(players.collect().head)

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

    // Determine the (1 - B) * e_N portion. The assumption is that e_N is a column vector
    // with 1 in a specific spot for target player
    val eN = refMapTuple.map(_ match {
      case (data, index) => {
        ((targetPlayer.value._2, index), 1.0 - BETA)
      }
    })

    // Create the transition matrix, using the key's unique id as the column and the edge's unique
    // id as the row. Calculate the value by dividing 1 (100%) by the number of edges for the key.
    // Then multiply it by beta (the chance the walker will walk at random. Afterwards, union
    // the initial matrix and reduce the values by the key getting the simplified transition
    // matrix.
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

    // Clean up the broadcast variable since it will not be used again
    refMap.unpersist(blocking = true)

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

    val nodeMap = sc.broadcast(mapData.map(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            (index, key)
          }
        }
      }
    }).collectAsMap)

    val solution = result.sortByKey().map(_ match {
      case (row, (col, sim)) => (nodeMap.value(row), (row, col), sim)
    })

    solution.collect().map(println(_))

    // Clean up
    sc.stop()
    spark.close()
  }
}
