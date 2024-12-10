#!/usr/bin/env bash

# 1. Before running this script: Run `stop-dfs.sh` and `stop-yarn.sh`, Start Hadoop services with: start-dfs.sh and start-yarn.sh
# - Check with jps to make sure NameNode, DataNode, ResourceManager, and NodeManager are running
# 2. Once the cluster is stable, run this script to:
# - Ensure the input/output directories in HDFS, load files to HDFS, compile the java classes and create a JAR, run the jobs

# name of the JAR file to create
JAR_NAME="stock-analysis.jar"

# input and output directories
INPUT_DIR="input"
OUTPUT_BASE="output"

# get current username for HDFS paths
CURRENT_USER=$(whoami)
HDFS_INPUT_DIR="/user/$CURRENT_USER/input"
HDFS_OUTPUT_DIR="/user/$CURRENT_USER/output"

# clean previous classes and output directories if needed
rm -rf classes
rm -rf "$OUTPUT_BASE"

# create classes and output directories
mkdir -p classes
mkdir -p "$OUTPUT_BASE"

# find all java source files
JAVA_FILES=$(find . -name "*.java")

# function: ensure HDFS directory exists
ensure_hdfs_directory() {
    local dir_path=$1
    echo "Checking if HDFS directory $dir_path exists..."
    if hdfs dfs -test -d "$dir_path"; then
        echo "HDFS directory $dir_path already exists."
    else
        echo "HDFS directory $dir_path does not exist. Creating it..."
        hdfs dfs -mkdir -p "$dir_path"
        if [ $? -ne 0 ]; then
            echo "Failed to create $dir_path. Exiting."
            exit 1
        fi
    fi
}

# assume Hadoop (HDFS + YARN) is already running and HDFS is accessible.
echo "Ensuring HDFS input directory..."
ensure_hdfs_directory "$HDFS_INPUT_DIR"

echo "Uploading local input files to HDFS..."
hdfs dfs -put -f "$INPUT_DIR"/* "$HDFS_INPUT_DIR"
if [ $? -ne 0 ]; then
    echo "Failed to upload files to $HDFS_INPUT_DIR. Check HDFS configuration."
    exit 1
fi

# Ensure output directory does not exist to avoid conflicts
if hdfs dfs -test -d "$HDFS_OUTPUT_DIR"; then
    echo "HDFS output directory $HDFS_OUTPUT_DIR already exists. Deleting it..."
    hdfs dfs -rm -r "$HDFS_OUTPUT_DIR"
fi

echo "Compiling Java files..."
javac -cp "$(hadoop classpath)" -d classes $JAVA_FILES
if [ $? -ne 0 ]; then
    echo "Compilation failed. Please check error messages above."
    exit 1
fi

echo "Creating JAR file: $JAR_NAME"
jar cvf "$JAR_NAME" -C classes .
if [ $? -ne 0 ]; then
    echo "JAR creation failed."
    exit 1
fi

echo "Compilation and JAR creation successful!"

# --- MapReduce Job Execution ---
echo "Running MapReduce jobs..."

# 1. MinMaxAnalysis
hadoop jar "$JAR_NAME" com.example.stockanalysis.MinMaxAnalysis "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/min_max"
echo "MinMaxAnalysis completed."

# 2. VolumeAverageAnalysis
hadoop jar "$JAR_NAME" com.example.stockanalysis.VolumeAverageAnalysis "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/volume"
echo "VolumeAverageAnalysis completed."

# 3. BiggestMoverAnalysis
hadoop jar "$JAR_NAME" com.example.stockanalysis.BiggestMoverAnalysis "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/movers"
echo "BiggestMoverAnalysis completed."

# 4. TrendingAnalysis (with -Dx=30)
hadoop jar "$JAR_NAME" com.example.stockanalysis.TrendingAnalysis -Dx=30 "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/trending"
echo "TrendingAnalysis completed."

# 5. Biggest in last year
hadoop jar "$JAR_NAME" com.example.stockanalysis.TopPerformer -Dx=365 "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/top"
echo "TopPerformer completed."


echo "All MapReduce jobs finished!"
