import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

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
    // This will be used to lookup the row/columns
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

    // Clean up the broadcast variable since it will not be used again
    targetPlayer.unpersist(blocking = true)

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
    
    // Continuously calculate the result matrix 5 times
    val firstIter = sc.broadcast(matrixMultiply(transition, initialIter).collect())
    initialIter.unpersist(blocking = true)
    val secondIter = sc.broadcast(matrixMultiply(transition, firstIter).collect())
    secondIter.unpersist(blocking = true)
    val thirdIter = sc.broadcast(matrixMultiply(transition, secondIter).collect())
    thirdIter.unpersist(blocking = true)
    val fourthIter = sc.broadcast(matrixMultiply(transition, thirdIter).collect())
    fourthIter.unpersist(blocking = true)
    val finalIter = matrixMultiply(transition, fourthIter).sortByKey().collect()

    /*
    players.collect().map(println(_))
    println("AKIRA")
    println(targetPlayer.value)
    finalIter.map(println(_))
    mapData.collect().map(println(_))
    */

    // Clean up
    sc.stop()
    spark.close()
  }
}
