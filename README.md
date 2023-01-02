# How to start GraphView

You can choose one of LogicBlox, PostgreSQL, or Neo4j as the underlying data engine for GraphView. In either case, LogicBlox should be running in advance.

## How to run LogicBlox
LogicBlox should be running before you execute GraphView.


## How to run PostgreSQL
PostgreSQL should be running in advance if you choose to run GraphView with PostgreSQL.
```bash
./start-dbs.sh
```

## How to run Neo4j
You should create some directories in advance.
```bash
mkdir ~/docker/neo4j/data -p
mkdir ~/docker/neo4j/logs -p 
mkdir ~/docker/neo4j/plugins -p
mkdir ~/docker/neo4j/import -p
```

You can start and stop Neo4j in docker
```bash
script/start-neo4j.sh
script/stop-neo4j.sh
```

## How to set up before execution

### The Z3 theorem prover
  1. Download the source code or pre-compiled binary release [here](https://github.com/Z3Prover/z3/releases). Unzip (and build, if needed) at `/path/to/z3libfolder`.
  1. The environment variable `LD_LIBRARY_PATH` should include the path that contains`libz3.so`. 
  1. The system property `java.libray.path` should include the path that contains `libz3java.so`. Both files are at `/path/to/z3libfolder/bin`. This should be set in the `pom.xml` for maven. Search `java.library.path` in the file.
  2. macOS's `DYLD_LIBRARY_PATH` doesn't work due to security reasons. Here is a workaround [[Link]](https://github.com/Z3Prover/z3/issues/294). 
```
put JNI dynamic link libraries in: /Library/Java/Extensions
e.g. libz3java.dylib
put none-JNI dynamic link libraries in: /usr/local/lib
e.g. libz3.dylib
```

## How to run the program


```bash
mvn compile
mvn exec:java     # to use LogicBlox for the underlying engine
mvn exec:java@pg  # to use PostgreSQL for the underlying engine
```


# Troubleshooting FAQ

## Program execution
1. 

## PostgresSQL

1. For large input data sets, you need to increase the size of the shared buffer size both for docker and for postgres 
    * Increase the `shared_buffer` value to e.g., `4GB` in `postgresql.conf` that is in your `$POSTGRES_DATA` directory that is mapped to `/data` in the docker container.
    * Increase the value of `shm-size` before you run the docker container for postgres. Refer to [link][1] for details.

        ```bash
        # in `start_db.sh`
        docker run --name some-postgres ...(options)... --shm-size=4g -d postgres       
        ```
 
[1]: https://medium.com/@shanikanishadhi1992/how-to-resolve-error-could-not-resize-shared-memory-segment-postgresql-no-space-left-on-device-8344c0ed3272
