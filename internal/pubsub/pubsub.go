package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Benshua/l-pubsub-s/handlers"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int 

const (
	Durable = iota 
	Transient
)


func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Error marshaling data (%v): %v", val, err)
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: b,
	})
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	mqChan, err := conn.Channel()
	handlers.FailOnError(err, "Failed to create channel for connection")

	var mqQueue amqp.Queue
	if queueType == Durable {
		mqQueue, err = mqChan.QueueDeclare(queueName, true, false, false, false, nil)
		handlers.FailOnError(err, "Failed to create queue for channel")
		}
	if queueType == Transient {
		mqQueue, err = mqChan.QueueDeclare(queueName, false, true, true, false, nil)
		handlers.FailOnError(err, "Failed to create queue for channel")
		}

	mqChan.QueueBind(mqQueue.Name, key, exchange, false, nil)

	return mqChan, mqQueue, nil
	}

func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T),
) error {
	ch, qu, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("There was an issue establishing either channel: %v or queue: %v. error: %v", ch, qu, err)
	}

	delivery, err  := ch.Consume(qu.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("There was an issue creating new channel %v, err: %v", delivery, err)
	}
		go func() {
			for d := range delivery {
				var data T
				err := json.Unmarshal(d.Body, &data)
				if err != nil {
					return //fmt.Errorf("Failed to unmarshal channel data: %v", err)
				}
				handler(data)
				d.Ack(false)
			}
		}()
	return nil
}

