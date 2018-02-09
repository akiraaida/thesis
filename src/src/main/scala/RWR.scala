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

    // Create the column matrix. Initially it is a matrix with all 0s except for the location
    // where the target player would be located
    val initialIter = sc.broadcast(Array(((targetPlayer.value._2, 0.toLong), 1.0)))

    // Determine the (1 - B) * e_N portion. The assumption is that e_N is a column vector
    // with 1 in a specific spot for target player
    val initial = refMapTuple.map(_ match {
      case (data, index) => {
        ((targetPlayer.value._2, index), 1 - BETA)
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
    }).union(initial).reduceByKey(_ + _)

    // Clean up the broadcast variable since it will not be used again
    refMap.unpersist(blocking = true)

    // A function used to multiply the matrices
    def matrixMultiply(transition: org.apache.spark.rdd.RDD[((Long, Long), Double)],
                  broadcast: org.apache.spark.broadcast.Broadcast[Array[((Long, Long), Double)]]): 
                                    org.apache.spark.rdd.RDD[((Long, Long), Double)] = {

      val result = transition.flatMap(_ match {
        case ((row, col), value) => {
          broadcast.value.map(_ match {
            case ((row2, col2), value2) => {
              if (col == row2) {
               ((row.toLong, 0.toLong), (value * value2).toDouble)
              } else {
               ((row.toLong, 0.toLong), 0.0)
              }
            }
          })
        }
      }).filter(_ match {
        case ((row, col), value) => value > 0
      }).reduceByKey(_ + _)

      result
    }
    
    // Continuously calculate the result matrix
    val iter1 = sc.broadcast(matrixMultiply(transition, initialIter).collect())
    initialIter.unpersist(blocking = true)
    val iter2 = sc.broadcast(matrixMultiply(transition, iter1).collect())
    iter1.unpersist(blocking = true)
    val iter3 = sc.broadcast(matrixMultiply(transition, iter2).collect())
    iter2.unpersist(blocking = true)
    val iter4 = sc.broadcast(matrixMultiply(transition, iter3).collect())
    iter3.unpersist(blocking = true)
    val iter5 = sc.broadcast(matrixMultiply(transition, iter4).collect())
    iter4.unpersist(blocking = true)
    val iter6 = sc.broadcast(matrixMultiply(transition, iter5).collect())
    iter5.unpersist(blocking = true)
    val iter7 = sc.broadcast(matrixMultiply(transition, iter6).collect())
    iter6.unpersist(blocking = true)
    val iter8 = sc.broadcast(matrixMultiply(transition, iter7).collect())
    iter7.unpersist(blocking = true)
    val iter9 = sc.broadcast(matrixMultiply(transition, iter8).collect())
    iter8.unpersist(blocking = true)
    val iter10 = sc.broadcast(matrixMultiply(transition, iter9).collect())
    iter9.unpersist(blocking = true)
    val iter11 = sc.broadcast(matrixMultiply(transition, iter10).collect())
    iter10.unpersist(blocking = true)
    val iter12 = sc.broadcast(matrixMultiply(transition, iter11).collect())
    iter11.unpersist(blocking = true)
    val iter13 = sc.broadcast(matrixMultiply(transition, iter12).collect())
    iter12.unpersist(blocking = true)
    val iter14 = sc.broadcast(matrixMultiply(transition, iter13).collect())
    iter13.unpersist(blocking = true)
    val iter15 = sc.broadcast(matrixMultiply(transition, iter14).collect())
    iter14.unpersist(blocking = true)
    val iter16 = sc.broadcast(matrixMultiply(transition, iter15).collect())
    iter15.unpersist(blocking = true)
    val iter17 = sc.broadcast(matrixMultiply(transition, iter16).collect())
    iter16.unpersist(blocking = true)
    val iter18 = sc.broadcast(matrixMultiply(transition, iter17).collect())
    iter17.unpersist(blocking = true)
    val iter19 = sc.broadcast(matrixMultiply(transition, iter18).collect())
    iter18.unpersist(blocking = true)

    // Remove the non-player nodes then map the index,key for later use
    val playerMapTuple = mapData.filter(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            key.contains("player")
          }
        }
      }
    }).map(_ match {
      case (data, index) => {
        data match {
          case (key, edges) => {
            (index, key)
          }
        }
      }
    })
    // Create a map for lookup
    val nodeMap = sc.broadcast(playerMapTuple.collectAsMap)

    // Remove the non-player nodes from the similarity rankings and remove the target player
    val iter20 = matrixMultiply(transition, iter19).sortByKey().filter(_ match {
      case ((key, _), similarity) => {
        nodeMap.value.get(key) != None
      }
    }).filter(_ match {
      case ((key, _), _) => {
        key != targetPlayer.value._2
      }
    })

    iter19.unpersist(blocking = true)
    nodeMap.unpersist(blocking = true)
    targetPlayer.unpersist(blocking = true)

    iter20.map(println(_))

    // Clean up
    sc.stop()
    spark.close()
  }
}
