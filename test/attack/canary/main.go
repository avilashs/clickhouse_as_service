// attack-canary listens on HTTP and raw TCP ports and logs every connection attempt it
// receives. The attack suite aims SSRF attempts at it and then asserts that no connection
// arrived: a deny that still let the request out would show up here.
package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	for _, port := range []string{"3306", "5432", "6379", "27017", "9000"} {
		go tcp(port)
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("HIT %s http %s %s", time.Now().UTC().Format(time.RFC3339), r.RemoteAddr, r.URL.Path)
		fmt.Fprintln(w, "canary")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func tcp(port string) {
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			continue
		}
		log.Printf("HIT %s tcp:%s %s", time.Now().UTC().Format(time.RFC3339), port, c.RemoteAddr())
		c.Close()
	}
}
