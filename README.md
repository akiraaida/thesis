### Undergraduate Thesis - Match Making with Random Walk with Reset
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
    - trunc.csv: A small segment of the data.csv used for testing.
- **doc/**: Reference material
    - book.pdf: Mining of Massive Datasets (http://www.mmds.org/)
- **src/**: The source code implementation of the solution.
