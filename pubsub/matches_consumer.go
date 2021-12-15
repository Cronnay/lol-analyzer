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

//MatchesConsumer qwe
type MatchesConsumer struct {
	c               *kafka.Consumer
	playersProducer *PlayersProducer
	sigchan         chan os.Signal
	db              *mongo.Client
}

//InitMatchesConsumer takes producer to produce messages to
func InitMatchesConsumer(kafkaConfig kafka.ConfigMap, playersProducer *PlayersProducer, db *mongo.Client) (*MatchesConsumer, error) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, err
	}

	subErr := c.SubscribeTopics([]string{"matches"}, nil)
	if subErr != nil {
		return nil, subErr
	}

	return &MatchesConsumer{c: c, playersProducer: playersProducer, sigchan: sigchan, db: db}, nil
}

//StartPolling will start and handle messages
func (c MatchesConsumer) StartPolling() {
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

func (c MatchesConsumer) handleMessage(value string) {
	db := os.Getenv("MONGO_DB")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := c.db.Database(db).Collection("matches").FindOne(ctx, bson.M{"matchID": value})

	data := make(map[string]string, 1)
	if decodeErr := res.Decode(&data); decodeErr != nil {
		if decodeErr == mongo.ErrNoDocuments {
			match, matchErr := getMatchFromMatchID(value)
			if matchErr != nil {
				fmt.Printf("Error: %v \n", matchErr)
			} else {
				participants := match["metadata"].(map[string]interface{})["participants"].([]interface{})
				for _, participant := range participants {
					p := participant.(string)
					if p != value {
						c.playersProducer.SendMessage(p)
					}
				}
				_, qErr := c.db.Database(db).Collection("processedMatches").InsertOne(ctx, match)
				if qErr != nil {
					fmt.Printf("Error: %v \n", qErr)
				} else {
					_, wErr := c.db.Database(db).Collection("matches").InsertOne(ctx, bson.M{"matchID": value})
					if wErr != nil {
						fmt.Printf("Error: %v \n", wErr)
					}
				}

			}
		} else {
			fmt.Printf("Error from mongodb: %v \n", decodeErr)
		}
	}
}

func getMatchFromMatchID(matchID string) (map[string]interface{}, error) {
	regionalRiotURL := os.Getenv("REGIONAL_RIOT_URL")
	apiKey := os.Getenv("RIOT_API_KEY")
	client := &http.Client{}
	req, requestBuilderError := http.NewRequest("GET", fmt.Sprintf("%s/lol/match/v5/matches/%s", regionalRiotURL, matchID), nil)
	if requestBuilderError != nil {
		fmt.Printf("Error when building: %s \n", requestBuilderError)
		return nil, requestBuilderError
	}
	req.Header.Set("X-Riot-Token", apiKey)
	res, resErr := client.Do(req)
	if resErr != nil {
		fmt.Printf("Error getting data from Riot. %s \n", resErr)
		return nil, resErr
	}

	if res.StatusCode != 200 {
		fmt.Printf("Not statuscode 200. We got: %d when getting match from match id\n", res.StatusCode)
		return nil, errors.New("Not 200")
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("Error when reading from io %v \n", err)
		return nil, err
	}

	var expectedType = make(map[string]interface{}, 0)
	parseErr := json.Unmarshal(body, &expectedType)
	if parseErr != nil {
		fmt.Printf("Error parsing when getting match from match id")
		return nil, parseErr
	}
	return expectedType, nil
}
