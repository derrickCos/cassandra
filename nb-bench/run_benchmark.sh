#!/bin/bash

# Function to check if JDK 17+ is installed
check_java_version() {
  java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ $java_version == 17.* || $java_version == 18.* || $java_version == 19.* || $java_version == 2[0-9].* ]]; then
    echo "JDK 17+ is installed."
    return 0
  else
    echo "JDK 17+ is not installed. Program exiting..."
    return 1
  fi
}

check_java_version

if [ ! -e "library/nb5.jar" ]; then
    cd library 
    curl -OL https://github.com/nosqlbench/nosqlbench/releases/download/5.17.5-preview/nb5.jar
    chmod +x nb5.jar
    cd ../
fi

if [ ! -d "testdata" ]; then
    DATASETS="glove-25-angular glove-50-angular glove-100-angular glove-200-angular deep-image-96-angular lastfm-64-dot"
    mkdir -p testdata
    pushd .
    cd testdata

    DATASET=${DATASETS?is required}

    for dataset in ${DATASETS}
    do
     URL="http://ann-benchmarks.com/${dataset}.hdf5"
     curl -OL "${URL}"
    done
    popd
fi

./recall.py
