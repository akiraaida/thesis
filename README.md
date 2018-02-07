### Undergraduate Thesis - Match Making with Random Walking with Reset
- Implementation of the Random Walk with Reset Algorithm to calculate similarity values for a player node.
- Code Implementation: Scala.
- Distributed Computing Framework: Apache Spark.
#### Software Used
##### The application may work with different software versions but this is what I had installed.
- Scala (Version 2.11.8)
- Java (Version 1.8.0_161)
- Spark (Version 2.2.0)
- sbt (Version 1.1.0)
#### Directory Structure
- **inc/**: The input and output data files
    - SkillCraft1_Dataset.csv: The raw dataset from online (http://archive.ics.uci.edu/ml/datasets/skillcraft1+master+table+dataset)
    - data.csv: The dataset with the first line (header) removed. Used as input for the PrepData.scala program.
    - transition.csv: The output of the PrepData.scala program which is a .csv file of (key, row, col, value) entries for the construction of the transition matrix.
- **doc/**: Reference material
    - book.pdf: Mining of Massive Datasets (http://www.mmds.org/)
- **src/**: The source code implementation of the solution.
    - **prep/**: The source code implementation of the conversion from the raw dataset to a transition matrix (distributed).
    - **rwr/**: The source code implementation of the Random Walk with Reset Algorithm (distributed).
#### Calculating the Transition Matrix
- You should be in ./src/prep (where the build.sbt is)
    - Execute 'sbt package'
    - For local execution 'spark-submit --class "PrepData" --master local[4] target/scala-2.11/prepdata_2.11-1.0.jar'
    - This will generate a directory 'transition' where a file 'part-0000' will contain the transition matrix.
#### Calculating the Similarity of a Node
- You should be in ./src/rwr (where the build.sbt is)
    - Execute 'sbt package'
    - For local execution 'spark-submit --class "RWR" --master local[4] target/scala-2.11/rwr_2.11-1.0.jar'
