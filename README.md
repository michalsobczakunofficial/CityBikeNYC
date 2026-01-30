# CitiBikeNYC (.NET 9)

Console app that imports Citi Bike NYC tripdata (`*.csv.zip`) into SQLite and runs analytic LINQ queries.

## Requirements
- .NET SDK 9.x

## Build
```bash
dotnet restore
dotnet build
```

## Import
Put the zip wherever you want (e.g. `data/` next to the project) and run:

```bash
dotnet run -c Release -- import "data\202407-citibike-tripdata.zip" --db "citibike.db" --batch 30000
```

## Run analytics
List available queries:

```bash
dotnet run -c Release -- stats --list --db "citibike.db"
```

Run a single query (e.g. Q3):

```bash
dotnet run -c Release -- stats --q 3 --db "citibike.db"
```

Run multiple:

```bash
dotnet run -c Release -- stats --q 1,2,8 --db "citibike.db"
```

Run all:

```bash
dotnet run -c Release -- stats --all --db "citibike.db"
```

