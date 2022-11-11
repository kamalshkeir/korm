package kmongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var dbConn *mongo.Client

var (
	// MMongoDBS keep track of mongo databases
	MMongoDBS = kmap.New[string, *mongo.Database](false)
	// MMongoClients keep track of mongo clients
	MMongoClients = kmap.New[string, *mongo.Client](false)
	// IsUsed check if mongo is used
	IsUsed = false
)

func NewMongoFromDSN(dbName string, dbDSN ...string) (*mongo.Database, error) {
	IsUsed = true
	var err error
	var client *mongo.Database
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if len(dbDSN) > 0 {
		dbConn, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+dbDSN[0]))
	} else {
		dbConn, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	}
	if klog.CheckError(err) {
		return nil, err
	}
	err = dbConn.Ping(ctx, readpref.Primary())
	if klog.CheckError(err) {
		return nil, err
	}
	if v, ok := MMongoClients.Get(dbName); ok {
		client = v.Database(dbName)
		return client, nil
	}
	if v, ok := MMongoDBS.Get(dbName); ok {
		client = v
		return client, nil
	}
	client = dbConn.Database(dbName)
	MMongoDBS.Set(dbName, client)
	MMongoClients.Set(dbName, dbConn)
	return client, nil
}

func NewMongoFromConnection(dbName string, dbConn *mongo.Client) (*mongo.Database, error) {
	IsUsed = true
	var mngoDB *mongo.Database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := dbConn.Ping(ctx, readpref.Primary())
	if klog.CheckError(err) {
		return nil, err
	}
	if v, ok := MMongoClients.Get(dbName); ok {
		mngoDB = v.Database(dbName)
		return mngoDB, nil
	}
	if v, ok := MMongoDBS.Get(dbName); ok {
		mngoDB = v
		return mngoDB, nil
	}
	mngoDB = dbConn.Database(dbName)
	MMongoDBS.Set(dbName, mngoDB)
	MMongoClients.Set(dbName, dbConn)
	return mngoDB, nil
}

func AddUniqueConstraint(table, column string, dbName ...string) error {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}

	_, err := client.Collection(table).Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bson.D{{Key: column, Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	)
	if klog.CheckError(err) {
		return err
	}
	return nil
}

func CreateRow(ctx context.Context, table string, rowData any, dbName ...string) error {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	_, err := client.Collection(table).InsertOne(ctx, rowData)
	if klog.CheckError(err) {
		return err
	}
	return nil
}

func DeleteRow(ctx context.Context, table string, whereFields map[string]any, dbName ...string) error {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	res, err := client.Collection(table).DeleteOne(ctx, whereFields)
	if klog.CheckError(err) {
		return err
	}
	if res.DeletedCount < 1 {
		return errors.New("no row found")
	}
	return nil
}

func GetAll[T any](ctx context.Context, table string, dbName ...string) ([]T, error) {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	cursor, err := client.Collection(table).Find(ctx, bson.M{})
	if klog.CheckError(err) {
		return nil, err
	}
	list := []T{}
	for cursor.Next(ctx) {
		m := *new(T)
		err = cursor.Decode(&m)
		list = append(list, m)
		if klog.CheckError(err) {
			return nil, err
		}
	}
	err = cursor.Close(ctx)
	if klog.CheckError(err) {
		return nil, err
	}
	return list, nil
}

func GetAllPaginated[T any](ctx context.Context, table string, elements_number string, page_num string, dbName ...string) ([]T, error) {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	findOptions := options.Find()
	elems_nums, err := strconv.ParseInt(elements_number, 10, 64)
	klog.CheckError(err)
	pg_num, err := strconv.ParseInt(page_num, 10, 64)
	klog.CheckError(err)
	findOptions.SetLimit(elems_nums)
	findOptions.SetSkip(elems_nums * (pg_num - 1))
	cursor, err := client.Collection(table).Find(ctx, bson.M{}, findOptions)
	if klog.CheckError(err) {
		return nil, err
	}
	list := []T{}
	for cursor.Next(ctx) {
		m := *new(T)
		err = cursor.Decode(&m)
		list = append(list, m)
		if klog.CheckError(err) {
			return nil, err
		}
	}
	err = cursor.Close(ctx)
	if klog.CheckError(err) {
		return nil, err
	}
	return list, nil
}

func GetRow[T any](ctx context.Context, table string, whereFields map[string]any, dbName ...string) (T, error) {
	var err error
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	row := *new(T)
	err = client.Collection(table).FindOne(ctx, whereFields).Decode(&row)
	if err != nil {
		return *new(T), err
	}
	return row, nil
}

func UpdateRow(ctx context.Context, table string, whereFields map[string]any, newRow any, dbName ...string) error {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	_, err := client.Collection(table).UpdateOne(ctx, whereFields, bson.M{
		"$set": newRow,
	})
	if klog.CheckError(err) {
		return err
	}
	return nil
}

func DropTable(ctx context.Context, table string, dbName ...string) error {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	err := client.Collection(table).Drop(ctx)
	if klog.CheckError(err) {
		return err
	}
	return nil
}

func GetAllTables(dbName ...string) []string {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	res, err := client.ListCollectionNames(context.Background(), bson.M{})
	if klog.CheckError(err) {
		return nil
	}
	return res
}

func GetAllColumns(table string, dbName ...string) map[string]string {
	var client *mongo.Database
	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}
	var row = map[string]any{}
	var res = map[string]string{}
	err := client.Collection(table).FindOne(context.Background(), bson.M{}).Decode(&row)
	if err != nil {
		return nil
	}
	for k, v := range row {
		switch v.(type) {
		case string:
			res[k] = "string"
		case bool:
			res[k] = "boolean"
		case time.Time:
			res[k] = "timestampz"
		case int, int64, int32:
			res[k] = "int"
		case float32, float64:
			res[k] = "float"
		default:
			res[k] = fmt.Sprintf("%T", v)
		}
	}
	return res
}

func GetClient() *mongo.Client {
	return dbConn
}

func Query[T any](ctx context.Context, table, selected string, whereFields map[string]any, elements_number, page_num int64, orderBy string, dbName ...string) ([]T, error) {
	var client *mongo.Database
	var cursor *mongo.Cursor

	if len(dbName) > 0 {
		client = dbConn.Database(dbName[0])
	} else {
		if MMongoDBS.Len() > 0 {
			client = dbConn.Database(MMongoDBS.Keys()[0])
		}
	}

	var d = map[string]any{}
	if whereFields != nil {
		d = whereFields
	}
	var err error
	var findOptions *options.FindOptions
	if elements_number != 0 {
		findOptions = options.Find()
		findOptions.SetLimit(elements_number)
		findOptions.SetSkip(elements_number * (page_num - 1))
	}

	if orderBy != "" {
		if findOptions == nil {
			findOptions = options.Find()
		}
		toSort := bson.D{}

		if strings.Contains(orderBy, ",") {
			sp := strings.Split(orderBy, ",")
			for _, s := range sp {
				if strings.HasPrefix(s, "-") {
					toSort = append(toSort, primitive.E{Key: strings.TrimSpace(s[1:]), Value: -1})
				} else if strings.HasPrefix(s, "+") {
					toSort = append(toSort, primitive.E{Key: strings.TrimSpace(s[1:]), Value: 1})
				} else {
					toSort = append(toSort, primitive.E{Key: strings.TrimSpace(s), Value: 1})
				}
			}
		} else {
			if strings.HasPrefix(orderBy, "-") {
				toSort = append(toSort, primitive.E{Key: strings.TrimSpace(orderBy[1:]), Value: -1})
			} else if strings.HasPrefix(orderBy, "+") {
				toSort = append(toSort, primitive.E{Key: strings.TrimSpace(orderBy[1:]), Value: 1})
			} else {
				toSort = append(toSort, primitive.E{Key: strings.TrimSpace(orderBy), Value: 1})
			}
		}
		findOptions.SetSort(toSort)
	}

	if selected != "" {
		if findOptions == nil {
			findOptions = options.Find()
		}
		toSelect := bson.D{}
		if strings.Contains(selected, ",") {
			sp := strings.Split(selected, ",")
			for _, s := range sp {
				toSelect = append(toSelect, primitive.E{
					Key:   strings.TrimSpace(s),
					Value: 1,
				})
			}
		} else {
			toSelect = append(toSelect, primitive.E{
				Key:   strings.TrimSpace(selected),
				Value: 1,
			})
		}
		findOptions.SetProjection(toSelect)
	}

	if findOptions != nil {
		cursor, err = client.Collection(table).Find(ctx, d, findOptions)
	} else {
		cursor, err = client.Collection(table).Find(ctx, d)
	}
	if klog.CheckError(err) {
		return nil, err
	}

	list := []T{}
	for cursor.Next(ctx) {
		m := *new(T)
		err = cursor.Decode(&m)
		list = append(list, m)
		if klog.CheckError(err) {
			return nil, err
		}
	}
	err = cursor.Close(ctx)
	if klog.CheckError(err) {
		return nil, err
	}
	return list, nil
}

func QueryOne[T any](ctx context.Context, table, selected string, whereFields map[string]any, elements_number, page_num int64, orderBy string, dbName ...string) (T, error) {
	v, err := Query[T](ctx, table, selected, whereFields, elements_number, page_num, orderBy, dbName...)
	if err != nil {
		return *new(T), err
	}
	if len(v) == 0 {
		return *new(T), errors.New("no data found")
	}
	return v[0], nil
}
