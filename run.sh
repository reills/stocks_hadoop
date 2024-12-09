#!/usr/bin/env bash

# --- Configuration ---

# Name of the JAR file to be created
JAR_NAME="stock-analysis.jar"

# Input and output directories (relative to the project root)
INPUT_DIR="input"
OUTPUT_BASE="output" 

# --- Compilation ---

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_BASE"

# Find all Java source files
JAVA_FILES=$(find . -name "*.java")

# Compile Java code using javac directly (more portable)
echo "Compiling Java files..."
javac -cp $(hadoop classpath) -d classes $JAVA_FILES

# Create JAR file
echo "Creating JAR file: $JAR_NAME"
jar -cvf "$JAR_NAME" -C classes .

echo "Compilation and JAR creation successful!"

# --- MapReduce Job Execution ---

echo "Running MapReduce jobs..."

# 1. MinMaxClose
hadoop jar "$JAR_NAME" com.example.stockanalysis.Q1MinMaxClose "$INPUT_DIR" "$OUTPUT_BASE/q1"
echo "Q1MinMaxClose completed."

# 2. VolumeChange
hadoop jar "$JAR_NAME" com.example.stockanalysis.Q2VolumeChange "$INPUT_DIR" "$OUTPUT_BASE/q2"
echo "Q2VolumeChange completed."

# 3. DailyTopMovers
hadoop jar "$JAR_NAME" com.example.stockanalysis.Q3DailyTopMovers "$INPUT_DIR" "$OUTPUT_BASE/q3"
echo "Q3DailyTopMovers completed."

# 4. Uptrend (with -Dx=30)
hadoop jar "$JAR_NAME" com.example.stockanalysis.Q4Uptrend -Dx=30 "$INPUT_DIR" "$OUTPUT_BASE/q4"
echo "Q4Uptrend completed."

echo "All MapReduce jobs finished!"