package main

import (
	"campaign-executor/internal/config"
	"fmt"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("%+v\n", cfg)
}
