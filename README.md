### Undergraduate Thesis - Match Making with Random Walking with Reset
- Implementation of the Random Walking with Reset algorithm to calculate similarity values for a player node.
- Code implementation is done in the programming language Scala.
- Distributed computing framework is using Apache Spark.
- Book used for reference: http://www.mmds.org/
- Data sourced from: http://archive.ics.uci.edu/ml/datasets/skillcraft1+master+table+dataset  
### Build Instructions
##### The application may work with different software versions but this is what I had installed.
- Scala (Version 2.11.8)
- Java (Version 1.8.0_161)
- Spark (Version 2.2.0)
- sbt (Version 1.1.0)
    - Add the location of each bin/ to your PATH variable.
#### Calculating the Transition Matrix
- You should be in ./src/prep (where the build.sbt is)
    - Execute 'sbt package'
    - For local execution 'spark-submit --class "PrepData" --master local[4] target/scala-2.11/prepdata_2.11-1.0.jar'
