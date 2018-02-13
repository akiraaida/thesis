# A counter to set each line of the file with a priority value
priority = 0
# A size multiplier so the file can be scaled up for testing
sizeMultiplier = 1

# Open the input and output files
inputFile = open("SkillCraft1_Dataset.csv", "r")
outputFile = open("data.csv", "w")

for _ in range(sizeMultiplier):
    # Used to remove the header in the starting csv file
    first = True
    # Set the input file pointer to the start
    inputFile.seek(0)
    for line in inputFile:
        if first == True:
            first = False
        else:
            # Append the priority to the end of the line
            line = line.replace("\n", "") + "," + str(priority) + "\n"
            outputFile.write(line)
            priority += 1

# Close the input and output files
inputFile.close()
outputFile.close()
