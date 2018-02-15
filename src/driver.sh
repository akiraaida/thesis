#!/bin/bash

# Compile the code
sbt package

# Attempt to match 10 players
for value in {1..10}
do
    # Delete the output directory if it exists
    if [ -d "output" ]; then
        rm -rf output/
    fi

    # Execute the Spark program
    spark-submit --class "RWR" --master local[4] target/scala-2.11/rwr_2.11-1.0.jar

    # Make the output of the Spark program the input for the next iteration
    mv ./output/part* ./inc/
    rm ./inc/data.csv
    cat ./inc/part* >> ./inc/data.csv
    rm ./inc/part*
done
