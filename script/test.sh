#
# sudo docker exec -it graphview-postgres psql -U postgres
# (password is for sudo...)
#
#
mvn exec:java@pg

sudo docker exec -it graphview-postgres psql -U postgres -c "\i /data/test.sql"

sudo chown sbnet21 test.sql
