package main

import (
	"fmt"
	"os"
	"os/signal"

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

	user, err:= gamelogic.ClientWelcome()
	handlers.FailOnError(err, "failed to connect to broker")

	_, _, err = pubsub.DeclareAndBind(
		mqCon,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, user),
		routing.PauseKey,
		pubsub.Transient,
	)
	handlers.FailOnError(err, "failed to connect to broker")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	_, ok := <-signalCh
	if ok {
		fmt.Println("program shutting down, closing connection...")
		mqCon.Close()
	}

}
