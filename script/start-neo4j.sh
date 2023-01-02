#
# sudo docker exec -it graphview-neo4j 
# (password is for sudo...)
#

start_neo4j() {
    export NEO4J=$HOME/docker/neo4j
    export NEO4J_DATA=$NEO4J/data
    export NEO4J_LOGS=$NEO4J/logs
    export NEO4J_IMPORT=$NEO4J/import
    export NEO4J_PLUGINS=$NEO4J/plugins

    export NEO4J_PASSWORD=neo4j@ # should not be 'neo4j'

    docker run --name graphview-neo4j --publish=7474:7474 --publish=7687:7687 \
        -v $NEO4J_DATA:/data \
        -v $NEO4J_LOGS:/logs \
        -v $NEO4J_IMPORT:/var/lib/neo4j/import \
        -v $NEO4J_PLUGINS:/plugins \
        --env NEO4J_AUTH=neo4j/$NEO4J_PASSWORD \
        --env NEO4J_apoc_import_file_enabled=true \
        --env NEO4J_dbms_security_procedures_unrestricted=apoc.trigger.*,apoc.* \
        -d neo4j:latest
}

# start_postgres () {
#     export POSTGRES_PASSWORD=postgres@
#     export POSTGRES_USER=postgres
#     export POSTGRES_DATA=$HOME/db
#     if [ ! -d $POSTGRES_DATA ]; then
#         mkdir $POSTGRES_DATA
#     fi
#     # See https://hub.docker.com/_/postgres/
#     docker run --name graphview-postgres --publish=5432:5432 \
#     --volume=$POSTGRES_DATA:/data \
#     --volume=/home/sbnet21/src/workspace/graph-trans/test.sql:/data/test.sql \
#     -e POSTGRES_USER=$POSTGRES_USER \
#     -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
#     -e PGDATA=/data \
#     --shm-size=4g \
#     -d postgres 
# }

#
echo "### Start Neo4j ###"
start_neo4j
