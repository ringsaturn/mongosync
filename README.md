# mongosync

```bash
go install github.com/ringsaturn/mongosync

mongosync -SourceDB foo -SourceCollection bar1 -TargetDB foo -TargetCollection bar2
```

```txt
Usage of mongosync:
  -BatchSize int
        batch size (default 20)
  -InChanSize int
        in chan buffer size (default 2000)
  -LastOIDHex string
        from which moid (default "000000000000000000000000")
  -OutChanSize int
        out chan buffer size (default 2000)
  -ReadSpeed int
        read QPS (default 500)
  -SourceCollection string
        which collection data come from
  -SourceDB string
        which database data come from
  -SourceURI string
        which instance data come from (default "mongodb://localhost:27017")
  -TargetCollection string
        which collection data come from
  -TargetDB string
        which database data come from
  -TargetURI string
        which instance data come from (default "mongodb://localhost:27017")
  -Workers int
        how many workers (default 10)
  -WriteSpeed int
        write QPS (default 100)
```
