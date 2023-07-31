package main

import (
	"6.5840/raft"
	"fmt"
)

func main() {
	for i := 0; i < 10; i++ {
		fmt.Println(raft.RandTime(200, 500))
		//fmt.Println(rand.Int() % 300)
	}
}
