package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cronnay/lol-crawler/pubsub"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	apiKey          string
	platformRiotURL string
	regionalRiotURL string
	db              *mongo.Client
	mProducer       *pubsub.MatchesProducer
	mConsumer       *pubsub.MatchesConsumer
	pProducer       *pubsub.PlayersProducer
	pConsumer       *pubsub.PlayersConsumer
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	apiKey = os.Getenv("RIOT_API_KEY")
	platformRiotURL = os.Getenv("PLATFORM_RIOT_URL")
	regionalRiotURL = os.Getenv("REGIONAL_RIOT_URL")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	initDb()

	kafaCfg := &kafka.ConfigMap{
		"bootstrap.servers":     "localhost:29092",
		"broker.address.family": "v4",
		"group.id":              "myGroup1",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	}
	var mProducerErr error
	mProducer, mProducerErr = pubsub.InitMatchesProducer(*kafaCfg)
	if mProducerErr != nil {
		log.Fatalf("Error: %s", mProducerErr.Error())
	}

	var pProducerErr error
	pProducer, pProducerErr = pubsub.InitPlayerProducer(*kafaCfg)
	if pProducerErr != nil {
		log.Fatalf("Error: %s", pProducerErr.Error())
	}

	var mConsumerErr error
	mConsumer, mConsumerErr = pubsub.InitMatchesConsumer(*kafaCfg, pProducer, db)
	if mConsumerErr != nil {
		log.Fatalf("Error: %s", mConsumerErr.Error())
	}
	go mConsumer.StartPolling()

	var pConsumerErr error
	pConsumer, pConsumerErr = pubsub.InitPlayerConsumer(*kafaCfg, mProducer, db)
	if pConsumerErr != nil {
		log.Fatalf("Error: %s", pConsumerErr.Error())
	}
	go pConsumer.StartPolling()

	p, puidErr := getPuuidFromAccountName("Cronnay")
	if puidErr != nil {
		log.Fatal(puidErr)
	}

	pProducer.SendMessage(p)
	<-ctx.Done()
}

func initDb() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	mongoURL := os.Getenv("MONGO_URL")
	db, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+mongoURL))
	if err != nil {
		log.Fatalf("Could not connect to MongoDB: %v", err)
	}

	if err = db.Ping(ctx, readpref.PrimaryPreferred()); err != nil {
		log.Fatalf("Could not ping Mongo after connecting: %v", err)
	}
}

type puuidStruct struct {
	ID            string `json:"id"`
	AccountID     string `json:"accountId"`
	Puuid         string `json:"puuid"`
	Name          string `json:"name"`
	ProfileIconID int    `json:"profileIconId"`
	RevisionDate  int64  `json:"revisionDate"`
	SummonerLevel int    `json:"summonerLevel"`
}

func getPuuidFromAccountName(name string) (string, error) {
	client := &http.Client{}
	req, requestBuilderError := http.NewRequest("GET", fmt.Sprintf("%s/lol/summoner/v4/summoners/by-name/%s", platformRiotURL, name), nil)
	if requestBuilderError != nil {
		fmt.Printf("Error when building: %s", requestBuilderError)
		return "", requestBuilderError
	}
	req.Header.Set("X-Riot-Token", apiKey)
	res, resErr := client.Do(req)
	if resErr != nil {
		fmt.Printf("Error getting data from Riot. %s", resErr)
		return "", resErr
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Print(err)
		return "", err
	}

	p := &puuidStruct{}
	json.Unmarshal(body, p)
	return p.Puuid, nil
}
