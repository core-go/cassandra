# cassandra
- Cassandra Utilities

## Installation
Please make sure to initialize a Go module before installing core-go/cassandra:

```shell
go get -u github.com/core-go/cassandra
```

Import:
```go
import "github.com/core-go/cassandra"
```
## Features
### SQL builder
- Insert, Update, Delete, Find By ID
#### Decimal
- Support decimal, which is useful for currency
### Batch
- Batch Insert
- Batch Batch Update
- Batch Insert or Update: support Oracle, PostgreSQL, My SQL, MS SQL Server, Sqlite
### Repository
- CRUD repository
