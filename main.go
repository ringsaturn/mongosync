package main

import (
	"context"
	"flag"
	"time"

	"github.com/ringsaturn/valve"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

var (
	// Original Mongo instance
	SourceURI        string
	SourceDB         string
	SourceCollection string

	// Target Mongo instance
	TargetURI        string
	TargetDB         string
	TargetCollection string

	// Last OID
	LastOIDHex string

	Workers int
)

func ok(err error) {
	if err != nil {
		panic(err)
	}
}

func worker(ctx context.Context, valveCore *valve.Core, coll *mongo.Collection) error {
	out, err := valveCore.Receive()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batchItem := <-out:
			valveCore.DoneInCounter()
			coll.InsertMany(ctx, batchItem)
		}
	}
}

func start(ctx context.Context) {
	sourceClient, err := mongo.NewClient(options.Client().ApplyURI(SourceURI))
	ok(err)

	err = sourceClient.Connect(ctx)
	ok(err)

	targetClient, err := mongo.NewClient(options.Client().ApplyURI(TargetURI))
	ok(err)

	err = targetClient.Connect(ctx)
	ok(err)

	cursor, err := sourceClient.Database(SourceDB).Collection(SourceCollection).Find(ctx, bson.M{})
	ok(err)

	// Buffer queue
	valveCore, err := valve.NewCore(time.NewTicker(100*time.Millisecond), 100, 2000, 2000)
	if err != nil {
		panic(err)
	}

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for cursor.Next(groupCtx) {
			record := &bson.M{}
			err = cursor.Decode(record)
			ok(err)
			valveCore.Add(groupCtx, record)
		}
		return nil
	})

	for i := 0; i < Workers; i++ {
		group.Go(func() error {
			return worker(groupCtx, valveCore, targetClient.Database(TargetDB).Collection(TargetCollection))
		})
	}
	ok(group.Wait())
}

func main() {
	ctx := context.Background()

	flag.StringVar(&SourceURI, "SourceURI", "mongodb://localhost:27017", "which instance data come from")
	flag.StringVar(&SourceDB, "SourceDB", "", "which database data come from")
	flag.StringVar(&SourceCollection, "SourceCollection", "", "which collection data come from")

	flag.StringVar(&TargetURI, "TargetURI", "mongodb://localhost:27017", "which instance data come from")
	flag.StringVar(&TargetDB, "TargetDB", "", "which database data come from")
	flag.StringVar(&TargetCollection, "TargetCollection", "", "which collection data come from")

	flag.StringVar(&LastOIDHex, "LastOIDHex", primitive.NilObjectID.Hex(), "from which moid")

	flag.IntVar(&Workers, "Workers", 10, "how many workers")

	flag.Parse()

	start(ctx)
}
