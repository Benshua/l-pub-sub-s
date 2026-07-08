package main

import (
	"fmt"
	"os"
	"os/signal"

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

	fmt.Println("Conenction successfull")
	fmt.Println("Starting Peril server...")

	mqChan, err := mqCon.Channel()
	if err != nil {
		fmt.Printf("There was an issue creating channel for %v: %v", mqCon, err)
	}

	e := routing.ExchangePerilDirect
	k := routing.PauseKey
	d := routing.PlayingState{ 
		IsPaused: true,
	}
	
	pubsub.PublishJSON(mqChan, e, k, d)
	

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	_, ok := <-signalCh
	if ok {
		fmt.Println("program shutting down, closing connection...")
		mqCon.Close()
	}

}
