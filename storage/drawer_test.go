package storage

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

type Data struct {
	Msg string
}

type DataSerializer struct {
}

func (s DataSerializer) Serialize(payload interface{}) ([]byte, error) {
	return json.Marshal(payload)
}

func (s DataSerializer) Deserialize(data []byte) (interface{}, error) {
	var payload Data
	err := json.Unmarshal(data, &payload)
	return &payload, err
}

var data = &Data{Msg: "hello"}
var serializer = DataSerializer{}
var filePath = "/Users/troublor/workspace/dArcher/refiler/data/drawer_test.txt"

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func TestNormalUsage(t *testing.T) {
	drawer := &Drawer{}
	defer os.RemoveAll(filePath)

	// write
	err := drawer.Activate(filePath, serializer)
	checkErr(err)
	drawer.Dump(data)
	err = drawer.Terminate()
	checkErr(err)

	// read
	drawer1 := &Drawer{}
	err = drawer1.Activate(filePath, serializer)
	checkErr(err)
	var data1 *Data
	data1, ok := drawer1.Expose().(*Data)
	if !ok {
		t.Fatal("when reading, data type is not correct")
	}
	if !reflect.DeepEqual(data, data1) {
		t.Fatal("when reading, data is different from what is written")
	}
}

func TestAutoFlush(t *testing.T) {
	drawer := &Drawer{}
	defer os.RemoveAll(filePath)

	// write
	err := drawer.Activate(filePath, serializer)
	checkErr(err)
	drawer.Dump(data)

	// wait for some time for it to flush
	time.Sleep(FlushInterval + time.Second)

	// check if the data has been flushed
	newDrawer := &Drawer{}
	err = newDrawer.Activate(filePath, serializer)
	checkErr(err)
	data1, ok := newDrawer.Expose().(*Data)
	if !ok {
		t.Fatal("when reading, data type is not correct")
	}
	if !reflect.DeepEqual(data, data1) {
		t.Fatal("data is not auto flushed")
	}
}

func TestStoreWithoutActivation(t *testing.T) {
	drawer := &Drawer{}
	defer func() {
		_ = recover()
	}()
	drawer.Dump(data)
	t.Fatal("not panic without activation")
}

/**
Every time drawer flushes, it should override previous content
*/
func TestWriteFileMultipleTimes(t *testing.T) {
	drawer := &Drawer{}
	drawer.Activate(filePath, serializer)
	drawer.Terminate()
	drawer.Activate(filePath, serializer)
	drawer.Terminate()
	err := drawer.Activate(filePath, serializer)
	if err != nil {
		panic(err)
	}
	drawer.Terminate()
	os.RemoveAll(filePath)
}
