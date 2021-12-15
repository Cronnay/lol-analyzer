package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

//PlayersConsumer qwe
type PlayersConsumer struct {
	c               *kafka.Consumer
	matchesProducer *MatchesProducer
	sigchan         chan os.Signal
	db              *mongo.Client
}

//InitPlayerConsumer takes producer to produce messages to
func InitPlayerConsumer(kafkaConfig kafka.ConfigMap, matchesProducer *MatchesProducer, db *mongo.Client) (*PlayersConsumer, error) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, err
	}

	subErr := c.SubscribeTopics([]string{"players"}, nil)
	if subErr != nil {
		return nil, subErr
	}

	return &PlayersConsumer{c: c, matchesProducer: matchesProducer, sigchan: sigchan, db: db}, nil
}

//StartPolling will start and handle messages
func (c PlayersConsumer) StartPolling() {
	run := true

	for run {
		select {
		case sig := <-c.sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.c.Poll(1000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.handleMessage(string(e.Value))
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Println("Closing consumer")
	c.c.Close()
}

func (c PlayersConsumer) handleMessage(value string) {
	db := os.Getenv("MONGO_DB")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := c.db.Database(db).Collection("players").FindOne(ctx, bson.M{"puuid": value})

	data := make(map[string]string, 1)
	if decodeErr := res.Decode(&data); decodeErr != nil {
		if decodeErr == mongo.ErrNoDocuments {
			matches, matchErr := getMatchesFromPuuid(value)
			if matchErr != nil {
				fmt.Printf("Error: %v \n", matchErr)
			} else {
				for _, matchID := range matches {
					c.matchesProducer.SendMessage(matchID)
				}
				_, wErr := c.db.Database(db).Collection("players").InsertOne(ctx, bson.M{"puuid": value})
				if wErr != nil {
					fmt.Printf("Error: %v \n", wErr)
				}
			}
		} else {
			fmt.Printf("Error from mongodb: %v \n", decodeErr)
		}
	}
}

func getMatchesFromPuuid(puuid string) ([]string, error) {
	regionalRiotURL := os.Getenv("REGIONAL_RIOT_URL")
	apiKey := os.Getenv("RIOT_API_KEY")
	client := &http.Client{}
	req, requestBuilderError := http.NewRequest("GET", fmt.Sprintf("%s/lol/match/v5/matches/by-puuid/%s/ids", regionalRiotURL, puuid), nil)
	if requestBuilderError != nil {
		fmt.Printf("Error when building: %s", requestBuilderError)
		return nil, requestBuilderError
	}
	req.Header.Set("X-Riot-Token", apiKey)
	res, resErr := client.Do(req)
	if resErr != nil {
		fmt.Printf("Error getting data from Riot. %s", resErr)
		return nil, resErr
	}

	if res.StatusCode != 200 {
		fmt.Printf("Not statuscode 200. We got: %d when getting matches from puuid\n", res.StatusCode)
		return nil, errors.New("Not 200")
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Print(err)
		return nil, err
	}
	var expectedType = make([]string, 0)
	parseErr := json.Unmarshal(body, &expectedType)
	if parseErr != nil {
		fmt.Printf("Error parsing when getting player from puuid")
		return nil, parseErr
	}

	return expectedType, nil
}
