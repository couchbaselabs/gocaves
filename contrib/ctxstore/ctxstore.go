package ctxstore

import (
	"reflect"
	"sync"
)

// Store allows generic contextual data storage
type Store struct {
	data     map[reflect.Type]reflect.Value
	dataLock sync.Mutex
}

// Get returns an associated context by its type
func (s *Store) Get(valuePtr interface{}) {
	v := reflect.ValueOf(valuePtr)
	if v.Kind() != reflect.Ptr {
		panic("should specify a pointer to pointer type (1)")
	}

	ptrV := reflect.Indirect(v)
	if ptrV.Kind() != reflect.Ptr {
		panic("should specify a pointer to pointer type (2)")
	}

	vType := ptrV.Type().Elem()
	s.dataLock.Lock()
	defer s.dataLock.Unlock()
	if s.data != nil {
		if ctx, ok := s.data[vType]; ok {
			ptrV.Set(ctx)
			return
		}
	} else {
		s.data = make(map[reflect.Type]reflect.Value)
	}

	newValPtr := reflect.New(vType)
	s.data[vType] = newValPtr

	ptrV.Set(newValPtr)
}
