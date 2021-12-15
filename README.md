# LOL Crawler

This project is a proof of concept. The entire project is to easily display how we can (try) to fetch ALL games and store the data.

## Issues that are not handled (Its a PoC)
* Rate limit (Can only handle 100 req every 2 mins)
* Doesn't handle multiple regions at the same time
* Doesn't put player/match into queue if something fails
* Messy code


## How does it work?
When starting the projects, it starts a 2x Kafka Producers and 2x Kafka Consumers. After starting everything, it will send a message with my PUUID (Player UUID) that I retrieve from my account name to `PlayersProducer`. `PlayersConsumer` fetches all recent 20 matches, and sends all match ids to `MatchesPublisher`. Then `MatchesConsumer` fetches data from a match id, store to DB. Extract all participants, and then send those PUUID's to `PlayerProducer` which will fetch 20 matches per player etc

## Requirements
* Docker and Docker Compose
* Go 1.16

## How do I get started?
1. Generate API-key from Riot Games. URL: https://developer.riotgames.com/
2. `git clone https://github.com/Cronnay/lol-crawler.git`
3. `cd lol-crawler`
4. `cp example.env .env`. Add your API-key. Update Riot URLs if not EUW
5. `docker-compose up`
6. `./run_dev.sh` or `go build && ./lol-crawler`
7. When you are getting error 429 its because of rate limit. 
8. Visit http://localhost:8081/db/lol_analyzer/ to have a look at the data