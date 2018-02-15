#!/bin/bash

# Create the inc directory if it doesn't exist
if [ ! -d "inc" ]; then
    mkdir inc
fi

# Delete the output directories if they exist
if [ -d "output" ]; then
    rm -rf output/
fi

if [ -d "matches" ]; then
    rm -rf matches/
fi

# Remove the contents of the inc directory
rm -rf inc/*

# Copy the original data file into the inc folder
cp ../inc/data.csv ./inc

# Compile the code
sbt package

# Match the players
for value in {1..10}
do
    # Execute the Spark program
    spark-submit --class "RWR" --master local[4] target/scala-2.11/rwr_2.11-1.0.jar

    # Make the output of the Spark program the input for the next iteration
    mv ./output/part* ./inc/
    rm ./inc/data.csv
    cat ./inc/part* >> ./inc/data.csv
    rm ./inc/part*

    # Save the pairings
    mv ./matches/part* ./inc/
    cat ./inc/part* >> ./inc/matches.txt
    echo "" >> ./inc/matches.txt
    rm ./inc/part*

    # Delete the output directories if they exist
    if [ -d "output" ]; then
        rm -rf output/
    fi

    if [ -d "matches" ]; then
        rm -rf matches/
    fi

done
