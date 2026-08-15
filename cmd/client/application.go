package main

import (
	"fmt"

	"github.com/Benshua/l-pubsub-s/internal/gamelogic"
	"github.com/Benshua/l-pubsub-s/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(gl gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(gl)
	}
}