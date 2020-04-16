package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

// Cluster is a persistent storage to store a very large array, the array will be split into several files
type Cluster struct {
	dir               string // the dirPath of the dir to store the cluster files
	prefix            string // prefix of each file name
	activated         bool
	elementSerializer Serializer
	close             chan interface{}

	chunks []*chunk

	chunkSize int // the number of elements in each file
}

func NewCluster(path string, serializer Serializer, chunkSize int, prefix string) (*Cluster, error) {
	cluster := &Cluster{
		dir:               path,
		prefix:            prefix,
		activated:         true,
		elementSerializer: serializer,
		chunks:            nil,

		chunkSize: chunkSize,
	}

	return cluster, nil
}

func (c *Cluster) Activate() error {
	// the dirPath needs to be a directory
	if d, err := os.Stat(c.dir); err == nil {
		// the directory exists
		if !d.IsDir() {
			return IsNotDirErr
		}
	} else {
		// create the dir
		err := os.MkdirAll(c.dir, 0777)
		if err != nil {
			return err
		}
	}

	c.chunks = make([]*chunk, 0)

	// initiate chunks
	for i := 0; ; i++ {
		if _, err := os.Stat(c.chunkPath(i)); err != nil {
			break
		} else {
			// chunk file exists
			// initiate chunk
			chunk := newChunk(c.chunkPath(i), c.elementSerializer, i, c.chunkSize)
			err = chunk.Activate()
			if err != nil {
				return err
			}
			c.chunks = append(c.chunks, chunk)
		}
	}

	c.activated = true
	return nil
}

func (c *Cluster) IsActivated() bool {
	return c.activated
}

func (c *Cluster) GetPath() string {
	c.checkActivated()
	return c.dir
}

func (c *Cluster) Terminate() error {
	c.checkActivated()
	c.activated = false
	// terminate all chunks
	for _, chunk := range c.chunks {
		err := chunk.Terminate()
		if err != nil {
			return err
		}
	}
	c.chunks = nil
	return nil
}

/**
Add an element at tail
*/
func (c *Cluster) Push(payload interface{}) error {
	if len(c.chunks) == 0 || c.chunks[len(c.chunks)-1].isFull() {
		// create a new chunk
		index := len(c.chunks)
		chunk := newChunk(c.chunkPath(index), c.elementSerializer, index, c.chunkSize)
		err := chunk.Activate()
		if err != nil {
			return err
		}
		tmp := make([]interface{}, 0)
		err = chunk.Dump(&tmp)
		if err != nil {
			return err
		}
		_ = chunk.push(payload)
		err = chunk.flush()
		if err != nil {
			return err
		}
		c.chunks = append(c.chunks, chunk)
	} else {
		_ = c.chunks[len(c.chunks)-1].push(payload)
	}
	return nil
}

func (c *Cluster) Get(i int) (interface{}, error) {
	if i < 0 || c.chunks == nil || len(c.chunks) == 0 {
		return nil, IndexOutOfBoundaryErr
	}
	for chunkIndex := 0; chunkIndex < len(c.chunks); chunkIndex++ {
		if i < c.chunks[chunkIndex].length() {
			elem, err := c.chunks[chunkIndex].get(i)
			if err != nil {
				return nil, err
			}
			return elem, nil
		}
		i -= c.chunks[chunkIndex].length()
	}
	return nil, IndexOutOfBoundaryErr
}

func (c *Cluster) Dump(payload interface{}) error {
	panic("not implemented")
}

func (c *Cluster) Expose() interface{} {
	panic("not implemented")
}

func (c *Cluster) checkActivated() {
	if !c.activated {
		panic(NotActivatedErr)
	}
}

func (c *Cluster) chunkPath(index int) string {
	return path.Join(c.dir, fmt.Sprintf("%s_chunk_%d.txt", c.prefix, index))
}

type chunk struct {
	*Drawer

	index    int // the index of the chunk in the cluster
	capacity int
}

func newChunk(path string, elementSerializer Serializer, index int, capacity int) *chunk {
	return &chunk{
		Drawer:   NewDrawer(path, newChunkSerializer(elementSerializer)),
		index:    index,
		capacity: capacity,
	}
}

func (c *chunk) isFull() bool {
	c.ensureActivated()
	return c.length() >= c.capacity
}

func (c *chunk) push(payload interface{}) error {
	c.ensureActivated()
	if c.isFull() {
		return ChunkFullErr
	}
	content := c.Expose().(*[]interface{})
	*content = append(*content, payload)
	_ = c.Dump(content)
	return nil
}

func (c *chunk) get(i int) (interface{}, error) {
	if i < 0 || i >= c.length() {
		return nil, IndexOutOfBoundaryErr
	}
	content := c.Expose().(*[]interface{})
	return (*content)[i], nil
}

func (c *chunk) ensureActivated() {
	if !c.IsActivated() {
		err := c.Activate()
		defer func() {
			err := c.Terminate()
			if err != nil {
				panic(err)
			}
		}()
		if err != nil {
			panic(err)
		}
	}
}

func (c *chunk) length() int {
	c.ensureActivated()
	content := c.Expose().(*[]interface{})
	return len(*content)
}

type chunkSerializer struct {
	elementSerializer Serializer
}

func newChunkSerializer(elementSerializer Serializer) *chunkSerializer {
	return &chunkSerializer{elementSerializer: elementSerializer}
}

func (s *chunkSerializer) Serialize(payload interface{}) ([]byte, error) {
	contents := payload.(*[]interface{})
	payloads := make([]string, len(*contents))
	for index, elem := range *contents {
		b, err := s.elementSerializer.Serialize(elem)
		if err != nil {
			return nil, err
		}
		payloads[index] = string(b)
	}
	return json.Marshal(payloads)
}

func (s *chunkSerializer) Deserialize(data []byte) (interface{}, error) {
	var payloads []string
	err := json.Unmarshal(data, &payloads)
	if err != nil {
		return nil, err
	}
	contents := make([]interface{}, len(payloads))
	for index, elem := range payloads {
		tmp, err := s.elementSerializer.Deserialize([]byte(elem))
		if err != nil {
			return nil, err
		}
		contents[index] = tmp
	}
	return &contents, nil
}
