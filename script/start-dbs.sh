#
# sudo docker exec -it graphview-postgres psql -U postgres
# (password is for sudo...)
#

start_postgres () {
    export POSTGRES_PASSWORD=postgres@
    export POSTGRES_USER=postgres
    export POSTGRES_DATA=$HOME/db
    if [ ! -d $POSTGRES_DATA ]; then
        mkdir $POSTGRES_DATA
    fi
    # See https://hub.docker.com/_/postgres/
    docker run --name graphview-postgres -itd --publish=5432:5432 \
    --volume=$POSTGRES_DATA:/data \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e PGDATA=/data \
    --shm-size=8g \
    --cpuset-cpus="0-1" \
    -d postgres 
}
# --volume=/home/sbnet21/src/workspace/graph-trans/test.sql:/data/test.sql \
#
echo "### Start Postgres ###"
start_postgres
