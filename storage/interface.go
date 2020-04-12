package storage

type Format string

type Serializer interface {
	Serialize(payload interface{}) ([]byte, error)
	Deserialize(data []byte) (interface{}, error)
}

/**
One PStore is a persistent storage that can only store one object
*/
type PStore interface {
	/**
	Start the drawer instance on path, if it doesn't exist on disk ,then create one
	*/
	Activate(path string, serializer Serializer) error

	IsActivated() bool

	/**
	Get the storage file path
	*/
	GetPath() string

	/**
	Terminate the drawer, write to file
	*/
	Terminate() error

	/**
	Dump value to drawer
	*/
	Dump(payload interface{}) error

	/**
	Get the value in drawer
	*/
	Expose() interface{}
}
