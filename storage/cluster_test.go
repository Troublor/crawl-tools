package storage

import (
	"os"
	"testing"
)

var dirPath = "/Users/troublor/workspace/go/go_module/crawler-tools/data/cluster_test"

func TestCluster_Push(t *testing.T) {
	defer os.RemoveAll(dirPath)
	cluster, err := NewCluster(dirPath, serializer, 2, "test")
	if err != nil {
		panic(err)
	}
	err = cluster.Activate()
	if err != nil {
		panic(err)
	}
	data0 := &Data{Msg: "0"}
	err = cluster.Push(data0)
	if err != nil {
		panic(err)
	}
	elem, err := cluster.Get(0)
	if err != nil {
		panic(err)
	}
	if elem.(*Data).Msg != "0" {
		t.Fatal("element wrong")
	}
	err = cluster.Terminate()
	if err != nil {
		panic(err)
	}

	// test read
	cluster, err = NewCluster(dirPath, serializer, 2, "test")
	if err != nil {
		panic(err)
	}
	err = cluster.Activate()
	if err != nil {
		panic(err)
	}
	elem, err = cluster.Get(0)
	if err != nil {
		panic(err)
	}
	if elem.(*Data).Msg != "0" {
		t.Fatal("element 0 wrong")
	}

	// test add more than chunkSize
	data1 := &Data{Msg: "1"}
	data2 := &Data{Msg: "2"}
	err = cluster.Push(data1)
	if err != nil {
		panic(err)
	}
	err = cluster.Push(data2)
	if err != nil {
		panic(err)
	}
	if len(cluster.chunks) != 2 {
		t.Fatal("chunks length is not 2")
	}

	// test get element in chunk[1]
	elem, err = cluster.Get(2)
	if err != nil {
		panic(err)
	}
	if elem.(*Data).Msg != "2" {
		t.Fatal("element 2 wrong")
	}

	err = cluster.Terminate()
	if err != nil {
		panic(err)
	}
}
