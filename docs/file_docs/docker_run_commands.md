# Quick run commands for docker thats good to have.

* To build container:
    * `docker-compose up --build`
* To build it "detatched" add -d before build:
    * `docker-compose up -d --build` 

* To start container:
    * `docker-compose up -d`

* To nuke it all and rebuild it:
    * `docker-compose down -v`

---

## To verify that the data is flowing

* Check logs, does the producer see the github events?
    * `docker logs producer --tail 20`

* Check consumer, does it write parquet to disk?
    * `docker logs consumer --tail 20`

* Check that Bronze folder is filling up:
    * `find data/bronze -name "*.parquet" | head -10`