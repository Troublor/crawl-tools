package storage

import (
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"
)

var FlushInterval = 5 * time.Second

/**
Drawer is a persistent storage struct implementation, which store an object in a file.
It is thread safe.
*/
type Drawer struct {
	path       string
	payload    interface{} // the payload should be a pointer to an object
	serializer Serializer  // elementSerializer is given by user, which is used to serialize and deserialize object
	activated  bool

	// flags to facilitate periodical flushing
	dirty         bool
	close         chan interface{}
	payloadRwLock sync.RWMutex
	flushLock     sync.Mutex
	terminateLock sync.Mutex
}

func NewDrawer(path string, serializer Serializer) *Drawer {
	return &Drawer{
		path:       path,
		payload:    nil,
		serializer: serializer,
		activated:  false,

		dirty: false,
	}
}

func (d *Drawer) GetPath() string {
	d.checkActivated()
	return d.path
}

func (d *Drawer) Activate() error {
	var err error
	// check if the dirPath already exist or not
	if fstat, err := os.Stat(d.path); err == nil {
		if fstat.IsDir() {
			return IsDirErr
		}
	}
	// create the file (if it doesn't exist)
	file, err := os.OpenFile(d.path, os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	// load the content in file
	err = d.load()
	if err == nil {
		d.close = make(chan interface{})
		d.activated = true
		go d.mainLoop()
	}
	return err
}

func (d *Drawer) IsActivated() bool {
	return d.activated
}

func (d *Drawer) Terminate() error {
	d.terminateLock.Lock()
	defer d.terminateLock.Unlock()
	if !d.IsActivated() {
		return nil
	}
	d.activated = false
	close(d.close)
	err := d.flush()
	return err
}

func (d *Drawer) Dump(payload interface{}) error {
	d.checkActivated()
	if !isPointer(payload) {
		panic(NonPointerErr)
	}
	d.payloadRwLock.Lock()
	d.payload = payload
	d.payloadRwLock.Unlock()
	d.dirty = true
	return nil
}

func (d *Drawer) Expose() interface{} {
	d.checkActivated()
	d.payloadRwLock.RLock()
	r := d.payload
	d.payloadRwLock.RUnlock()
	return r
}

/**
flush the payload to file (persistent storage)
*/
func (d *Drawer) flush() error {
	d.payloadRwLock.RLock()
	data, err := d.serializer.Serialize(d.payload)
	d.payloadRwLock.RUnlock()
	if err != nil {
		return err
	}
	d.flushLock.Lock()
	err = ioutil.WriteFile(d.path, data, 0644)
	d.flushLock.Unlock()
	if err == nil {
		d.dirty = false
	}
	return err
}

/**
load the content in the file and deserialize it
*/
func (d *Drawer) load() error {
	data, err := ioutil.ReadFile(d.path)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		var payload interface{}
		payload, err = d.serializer.Deserialize(data)
		if !isPointer(payload) {
			panic(NonPointerErr)
		}
		d.payload = payload
	} else {
		d.payload = nil
	}
	return err
}

func (d *Drawer) checkActivated() {
	if !d.activated {
		panic(NotActivatedErr)
	}
}

/**
the main loop for each drawer
the loop will periodically flush payload to file and monitor to INT signal, and terminate when got INT signal
*/
func (d *Drawer) mainLoop() {
	timer := time.After(FlushInterval)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	for {
		select {
		case <-d.close:
			return
		case <-sig:
			// got an interrupt signal from OS
			err := d.Terminate()
			if err != nil {
				panic(err)
			}
		case <-timer:
			if d.dirty {
				err := d.flush()
				if err != nil {
					panic(err)
				}
			}
			timer = time.After(FlushInterval)
		}
	}
}

func isPointer(v interface{}) bool {
	return reflect.ValueOf(v).Kind() == reflect.Ptr
}
