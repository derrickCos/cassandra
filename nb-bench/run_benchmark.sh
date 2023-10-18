#!/bin/bash

if [ ! -e "library/nb5.jar" ]; then
    cd library
    curl -OL https://github.com/nosqlbench/nosqlbench/releases/download/5.21.1-preview/nb5.jar
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

echo Drop schema
java -jar ././library/./nb5.jar cql-vector2 cassandra.drop host=localhost localdc=datacenter1 dataset=glove-100-angular --report-csv-to=././artifacts/glove-100_nb5_logs/metrics:test.*:5s --report-interval=1 keyspace=vector_test dimensions=100 --show-stacktraces 2>&1 | grep '%'

echo Create schema
java -jar ././library/./nb5.jar cql-vector2 cassandra.schema host=localhost localdc=datacenter1 dataset=glove-100-angular --report-csv-to=././artifacts/glove-100_nb5_logs/metrics:.*:5s --report-interval=1 keyspace=vector_test dimensions=100 --show-stacktraces 2>&1 | grep '%'

echo Train
java -jar ././library/./nb5.jar cql-vector2 cassandra.rampup host=localhost localdc=datacenter1 dataset=glove-100-angular trainsize=1183514 --report-csv-to=././artifacts/glove-100_nb5_logs/metrics:.*:5s --report-interval=1 keyspace=vector_test dimensions=100 --show-stacktraces 2>&1 | grep '%'

echo Test
java -jar ././library/./nb5.jar cql-vector2 cassandra.read_recall host=localhost localdc=datacenter1 dataset=glove-100-angular testsize=10000 --report-csv-to=././artifacts/glove-100_nb5_logs/metrics:.*:5s --report-interval=1 keyspace=vector_test dimensions=100 --show-stacktraces 2>&1 | grep '%'

./summarize.py
