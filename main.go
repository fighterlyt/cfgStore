package main

import (
	"github.com/fighterlyt/cfgStore/store"
	"github.com/fighterlyt/cfgStore/web"
	"log"
)

func main() {
	s, err := store.NewMongoStore("mongodb://my_user:password123@localhost:27018/orderbook", "test", "test")
	if err != nil {
		log.Fatal(err.Error())
	}
	if err = s.Init(); err != nil {
		log.Fatal(err.Error())
	}

	server := web.NewServer("1234", s)
	server.Start()
}
