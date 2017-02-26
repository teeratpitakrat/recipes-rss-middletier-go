package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/codegangsta/negroni"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/mmcdole/gofeed"
	opentracing "github.com/opentracing/opentracing-go"
	"sourcegraph.com/sourcegraph/appdash"
	appdashtracer "sourcegraph.com/sourcegraph/appdash/opentracing"
)

const CtxSpanID = 0

var collector appdash.Collector

var cassandraAddr string

type Subscription struct {
	Feeds []*gofeed.Feed
}

func FetchFeed(w http.ResponseWriter, req *http.Request) {
	carrier := opentracing.HTTPHeadersCarrier(req.Header)
	spanCtx, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		return
	}
	span := opentracing.StartSpan("middletier:FetchFeed", opentracing.ChildOf(spanCtx))
	defer span.Finish()

	vars := mux.Vars(req)
	user := vars["user"]
	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	feedURLs, err := GetUrls(ctx, user)
	if err != nil {
		ReturnErrorPage(w, req, err)
		return
	}
	subscription := Subscription{}
	for _, feedURL := range feedURLs {
		feed, err := FetchFeedContents(ctx, feedURL)
		if err != nil {
			continue
		}
		feed.FeedLink = feedURL
		subscription.Feeds = append(subscription.Feeds, feed)
	}
	json.NewEncoder(w).Encode(subscription)
}

func GetUrls(ctx context.Context, user string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "middletier:GetUrls")
	defer span.Finish()

	session, err := GetCassandraSession(ctx)
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

func FetchFeedContents(ctx context.Context, feedURL string) (*gofeed.Feed, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "middletier:FetchFeedContents")
	span.SetTag("URL", feedURL)
	defer span.Finish()

	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(feedURL)
	if err != nil {
		return nil, err
	}
	return feed, nil
}

func Subscribe(w http.ResponseWriter, req *http.Request) {
	carrier := opentracing.HTTPHeadersCarrier(req.Header)
	spanCtx, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		return
	}
	span := opentracing.StartSpan("middletier:Subscribe", opentracing.ChildOf(spanCtx))
	defer span.Finish()

	vars := mux.Vars(req)
	user := vars["user"]
	feedURL := req.FormValue("url")

	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	session, err := GetCassandraSession(ctx)
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

func Unsubscribe(w http.ResponseWriter, req *http.Request) {
	carrier := opentracing.HTTPHeadersCarrier(req.Header)
	spanCtx, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		return
	}
	span := opentracing.StartSpan("middletier:Unsubscribe", opentracing.ChildOf(spanCtx))
	defer span.Finish()

	vars := mux.Vars(req)
	user := vars["user"]
	feedURL := req.FormValue("url")

	ctx := context.Background()
	ctx = opentracing.ContextWithSpan(ctx, span)
	session, err := GetCassandraSession(ctx)
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

func GetCassandraSession(ctx context.Context) (*gocql.Session, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "middletier:GetCassandraSession")
	defer span.Finish()

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
	collector = appdash.NewRemoteCollector("appdash:7701")
	collector = appdash.NewChunkedCollector(collector)
	tracer := appdashtracer.NewTracer(collector)
	opentracing.InitGlobalTracer(tracer)

	cassandraAddr = os.Getenv("CASSANDRA_ADDR")
	fmt.Println("cassandra addr:", cassandraAddr)

	router := mux.NewRouter()
	router.HandleFunc("/middletier/rss/user/{user}", FetchFeed).Methods("GET")
	router.HandleFunc("/middletier/rss/user/{user}", Subscribe).Methods("POST")
	router.HandleFunc("/middletier/rss/user/{user}", Unsubscribe).Methods("DELETE")
	router.HandleFunc("/healthcheck", Healthcheck)

	n := negroni.Classic()
	n.UseHandler(router)
	n.Run(":9191")
}
