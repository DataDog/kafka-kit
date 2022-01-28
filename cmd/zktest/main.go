package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func main() {
	zk := flag.String("zk", "localhost:2181", "zookeeper")
	flag.Parse()

	z, err := kafkazk.NewHandler(&kafkazk.Config{Connect: *zk})
	exitOnErr(err)

	m, err := z.ListReassignments()
	exitOnErr(err)

	fmt.Printf("%+v\n", m)
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
