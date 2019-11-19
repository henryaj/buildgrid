# Stress testing of BuildGrid

## How to run

Choose one of the included `docker-compose-*.yml` to test with.

Available options include:
*  `docker-compose-pges-recc-bbrht.yml`
*  `docker-compose-pges-indexedcas-recc-bbrht.yml`
*  `docker-compose-sqlite-recc-bbrht.yml`
*  `docker-compose-sqlite-indexedcas-recc-bbrht.yml`


You can build and run the docker-compose files as follows:
```bash
STDC=docker-compose-of-your-choice.yml
# e.g.
# STDC=docker-compose-pges-recc-bbrht.yml
docker-compose -f $STDC build

docker-compose -f $STDC up -d --scale clients=100 --scale bots=10

# To see the status of all the containers
docker-compose -f $STDC ps

# To see the status of all the containers that have NOT exited 0
docker-compose -f $STDC ps | grep -v "Exit 0"
# ... Or watch using the provided script
watch -n 0.5 -d scripts/stdc-ps-non0.sh
```
