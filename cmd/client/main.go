package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Benshua/l-pubsub-s/handlers"
	"github.com/Benshua/l-pubsub-s/internal/gamelogic"
	"github.com/Benshua/l-pubsub-s/internal/pubsub"
	"github.com/Benshua/l-pubsub-s/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)



func main() {

	CONNECTION_STRING := "amqp://guest:guest@localhost:5672/"

	mqCon, err := amqp.Dial(CONNECTION_STRING)
	handlers.FailOnError(err, "failed to connect to broker")

	defer mqCon.Close()
	fmt.Println("Conenction successfull")
	fmt.Println("Starting Peril client...")

	username, err := gamelogic.ClientWelcome()
	handlers.FailOnError(err, "failed to establish peril_direct binding")

	pubChan, err := mqCon.Channel()
	if err != nil {
		fmt.Printf("There was an issue creating channel for %v: %v", mqCon, err)
	}

	/*
	// This code is replaced by pubsub.SubscribeJson

	_, _, err = pubsub.DeclareAndBind(
		mqCon,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		username,
		pubsub.Transient,
	)
	handlers.FailOnError(err, "failed to establish peril_topic binding")
	*/

	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		mqCon,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%v", username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gamestate),
	)
	if err != nil {
		fmt.Printf("Error occured subscribing to topic, %v", err)
		return
	
	}
	err = pubsub.SubscribeJSON(
		mqCon,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%v.%v", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%v.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(gamestate),
	)
	if err != nil {
		fmt.Printf("Error occured subscribing to topic, %v", err)
		return
	}
	

	commands := map[string]struct{}{
		"spawn": {},
		"move": {},
		"status": {},
		"help": {},
		"spam": {},
		"quit": {},

	}
	loop:
	for {
		words := gamelogic.GetInput()
		if _, ok := commands[words[0]]; !ok {
			log.Println("invalid command")
		}
		
		switch strings.ToLower(words[0]) {
		case "spawn":
			log.Println("spawning unit...")
			err = gamestate.CommandSpawn(words)
			if err != nil {
				log.Println(err)
			}
			
		//	handlers.FailOnError(err, "failed to spawn unit")	

		case "move":
			log.Println("moving unit...")	
			mv, err := gamestate.CommandMove(words)
			if err != nil {
				log.Println(err)
			}
			pubsub.PublishJSON(pubChan, routing.ExchangePerilTopic, fmt.Sprintf("%v.*", routing.ArmyMovesPrefix), mv )

		//	handlers.FailOnError(err, "failed to move unit")

		case "status":
			gamestate.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("spam not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			break loop
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
