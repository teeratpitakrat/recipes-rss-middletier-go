package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/mmcdole/gofeed"
	"github.com/teeratpitakrat/gokieker"
)

var cassandraAddr string

type Subscription struct {
	Feeds []*gofeed.Feed
}

func GetFeed(w http.ResponseWriter, req *http.Request) {
	k := gokieker.BeginFunction()
	defer k.EndFunction()

	vars := mux.Vars(req)
	user := vars["user"]
	feedURLs, err := GetUrls(user)
	if err != nil {
		ReturnErrorPage(w, req, err)
		return
	}
	subscription := Subscription{}
	for _, feedURL := range feedURLs {
		fp := gofeed.NewParser()
		feed, _ := fp.ParseURL(feedURL)
		feed.FeedLink = feedURL
		subscription.Feeds = append(subscription.Feeds, feed)
	}
	json.NewEncoder(w).Encode(subscription)
}

func GetUrls(user string) ([]string, error) {
	k := gokieker.BeginFunction()
	defer k.EndFunction()

	session, err := GetCassandraSession()
	if err != nil {
		return nil, errors.New("Cannot connect to cassandra")
	}
	defer session.Close()

	var feedURL string
	feedURLs := make([]string, 0)
	iter := session.Query(`SELECT column1 FROM "Subscriptions" WHERE key = ?`, user).Iter()
	for iter.Scan(&feedURL) {
		feedURLs = append(feedURLs, feedURL)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
		return nil, errors.New("Error fetching data from cassandra")
	}

	return feedURLs, nil
}

func AddFeed(w http.ResponseWriter, req *http.Request) {
	k := gokieker.BeginFunction()
	defer k.EndFunction()

	vars := mux.Vars(req)
	user := vars["user"]
	feedURL := req.FormValue("url")

	session, err := GetCassandraSession()
	if err != nil {
		log.Fatal(err)
		ReturnErrorPage(w, req, err)
		return
	}
	defer session.Close()
	err = session.Query(`INSERT INTO "Subscriptions" (key, column1, value) VALUES (?, ?, ?)`,
		user, feedURL, "1").Exec()
	if err != nil {
		log.Fatal(err)
		ReturnErrorPage(w, req, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func DeleteFeed(w http.ResponseWriter, req *http.Request) {
	k := gokieker.BeginFunction()
	defer k.EndFunction()

	vars := mux.Vars(req)
	user := vars["user"]
	feedURL := req.FormValue("url")

	session, err := GetCassandraSession()
	if err != nil {
		log.Fatal(err)
		ReturnErrorPage(w, req, err)
		return
	}
	defer session.Close()
	err = session.Query(`DELETE FROM "Subscriptions" WHERE key=? AND column1=?`,
		user, feedURL).Exec()
	if err != nil {
		log.Fatal(err)
		ReturnErrorPage(w, req, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func GetCassandraSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster(cassandraAddr)
	cluster.Keyspace = "RSS"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	return session, err
}

func ReturnErrorPage(w http.ResponseWriter, req *http.Request, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func Healthcheck(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<h1>Healthcheck page</h1>")
}

func main() {
	gokieker.StartMonitoring()
	k := gokieker.BeginFunction()
	defer k.EndFunction()

	cassandraAddr = os.Getenv("CASSANDRA_ADDR")
	fmt.Println("cassandra addr:", cassandraAddr)

	r := mux.NewRouter()
	r.HandleFunc("/middletier/rss/user/{user}", GetFeed).Methods("GET")
	r.HandleFunc("/middletier/rss/user/{user}", AddFeed).Methods("POST")
	r.HandleFunc("/middletier/rss/user/{user}", DeleteFeed).Methods("DELETE")
	r.HandleFunc("/healthcheck", Healthcheck)

	srv := &http.Server{
		Handler:      r,
		Addr:         ":9191",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
