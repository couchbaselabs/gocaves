package ctxstore

import "testing"

type testTypeX struct {
	foo int
}

type testTypeY struct {
	bar string
	baz float32
}

func TestCtxStore(t *testing.T) {
	s := &Store{}

	var t1x *testTypeX
	var t2y *testTypeY
	var t3x *testTypeX

	// Check we can get a type
	s.Get(&t1x)
	if t1x.foo != 0 {
		t.Error("t1x was not default inited")
	}

	// Check we can get/set values
	t1x.foo = 13
	if t1x.foo != 13 {
		t.Errorf("t1x.foo was not 13")
	}

	// Check we can get the type again
	s.Get(&t3x)
	if t3x.foo != 13 {
		t.Errorf("t3x.foo was not 13")
	}

	// Check that setting one type updates the other
	t3x.foo = 99
	if t1x.foo != 99 {
		t.Errorf("t1x.foo was not 99")
	}

	// Check we can get a second type
	s.Get(&t2y)
	t2y.bar = "hello"

	// Check that the second type gets correctly
	s.Get(&t2y)
	if t2y.bar != "hello" {
		t.Errorf("t2y.bar was not `hello`")
	}

	// Check we can still get the first type
	s.Get(&t3x)
	if t3x.foo != 99 {
		t.Error("t3x.foo was not 99")
	}
}
