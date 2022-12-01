// MongoSync is tool for sync data between Mongo instances.
package main

import (
	"context"
	"flag"
	"log"
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

	InChanSize  int
	OutChanSize int
	BatchSize   int

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
			_, err = coll.InsertMany(ctx, batchItem)
			if err != nil {
				log.Println("failed", err)
				continue
			}
			log.Println("insert done", len(batchItem))
		default:
			time.Sleep(time.Second)
		}
	}
}

func start(ctx context.Context) {
	sourceClient, err := mongo.NewClient(options.Client().ApplyURI(SourceURI))
	ok(err)
	err = sourceClient.Connect(ctx)
	ok(err)
	log.Println("sourceClient init OK")

	targetClient, err := mongo.NewClient(options.Client().ApplyURI(TargetURI))
	ok(err)
	err = targetClient.Connect(ctx)
	ok(err)
	log.Println("targetClient init OK")

	query := bson.M{}
	if LastOIDHex != primitive.NilObjectID.Hex() {
		oid, err := primitive.ObjectIDFromHex(LastOIDHex)
		ok(err)
		query["_id"] = bson.M{"$gt": oid}
	}
	cursor, err := sourceClient.Database(SourceDB).Collection(SourceCollection).Find(ctx, query)
	ok(err)

	// Buffer queue
	valveCore, err := valve.NewCore(time.NewTicker(100*time.Millisecond), int64(BatchSize), InChanSize, OutChanSize)
	if err != nil {
		panic(err)
	}

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return valveCore.Start(groupCtx)
	})

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
	flag.IntVar(&InChanSize, "InChanSize", 2000, "in chan buffer size")
	flag.IntVar(&OutChanSize, "OutChanSize", 2000, "out chan buffer size")
	flag.IntVar(&BatchSize, "BatchSize", 20, "batch size")

	flag.Parse()

	start(ctx)
}
