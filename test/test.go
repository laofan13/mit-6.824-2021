package main

import (
	"fmt"
	"time"
)

func main() {
	t := time.Now()
	fmt.Printf("t = %v \n", t)
	t = t.Add(time.Millisecond * 1000)
	fmt.Printf("t + 1s = %v \n", t)

	for {
		now := time.Now()
		fmt.Printf("now = %v \n", now)

		if now.After(t) {
			fmt.Printf("now after t \n")
			break
		} else {
			fmt.Printf("now before t \n")
		}

		ms := time.Millisecond * 100
		time.Sleep(ms)
	}

	return
}
