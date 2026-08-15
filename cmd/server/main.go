package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Benshua/l-pubsub-s/internal/gamelogic"
	"github.com/Benshua/l-pubsub-s/internal/pubsub"
	"github.com/Benshua/l-pubsub-s/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	CONNECTION_STRING := "amqp://guest:guest@localhost:5672/"

	mqCon, err := amqp.Dial(CONNECTION_STRING)	
	if err != nil {
		fmt.Printf("THere was an issue connecting to RabbitMQ Broker: %v", err)
		return 
	}
	defer mqCon.Close()

	_, _, err = pubsub.DeclareAndBind(
		mqCon,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		"*",
		pubsub.Durable,
	)

	fmt.Println("Conenction successfull")
	fmt.Println("Starting Peril server...")

	mqChan, err := mqCon.Channel()
	if err != nil {
		fmt.Printf("There was an issue creating channel for %v: %v", mqCon, err)
	}

	gamelogic.PrintServerHelp()
	loop:
	for {
		fmt.Println("Entering infinite loop")
		user := gamelogic.GetInput()

		if len(user) == 0 {
			continue
		}
		switch strings.ToLower(user[0]) {
		case "pause":
			log.Println("sending pause message")
				e := routing.ExchangePerilDirect
				k := routing.PauseKey
				d := routing.PlayingState{ 
					IsPaused: true,
				}
				pubsub.PublishJSON(mqChan, e, k, d)
		case "resume":
			log.Println("sending resume message")
				e := routing.ExchangePerilDirect
				k := routing.PauseKey
				d := routing.PlayingState{ 
					IsPaused: false,
				}
				pubsub.PublishJSON(mqChan, e, k, d)
		case "quit":
			log.Println("sending exit message")
			break loop
		default:
			log.Println("invalid command, please consult help menu")
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	_, ok := <-signalCh
	if ok {
		fmt.Println("program shutting down, closing connection...")
		mqCon.Close()
	}

}
