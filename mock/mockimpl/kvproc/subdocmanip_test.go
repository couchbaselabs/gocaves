package kvproc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubdocManipObj(t *testing.T) {
	docRoot, err := newSubDocManip([]byte(`{"a":{"b":{"c":"foo"}}}`))
	assert.NoError(t, err)

	abc, err := docRoot.GetByPath("a.b.c", false, false)
	assert.NoError(t, err)

	abcJSON, err := abc.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, abcJSON, []byte(`"foo"`))

	err = abc.Set("bar")
	assert.NoError(t, err)

	docJSON1, err := docRoot.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, docJSON1, []byte(`{"a":{"b":{"c":"bar"}}}`))

	ade, err := docRoot.GetByPath("a.d.e", true, true)
	assert.NoError(t, err)

	err = ade.Set("baz")
	assert.NoError(t, err)

	docJSON2, err := docRoot.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, docJSON2, []byte(`{"a":{"b":{"c":"bar"},"d":{"e":"baz"}}}`))
}

func TestSubdocManipArr(t *testing.T) {
	docRoot, err := newSubDocManip([]byte(`{"a":["b","c","d","e"]}`))
	assert.NoError(t, err)

	a0, err := docRoot.GetByPath("a[0]", false, false)
	assert.NoError(t, err)

	a0JSON, err := a0.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, a0JSON, []byte(`"b"`))

	aN1, err := docRoot.GetByPath("a[-1]", false, false)
	assert.NoError(t, err)

	aN1JSON, err := aN1.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, aN1JSON, []byte(`"e"`))

	err = aN1.Set("f")
	assert.NoError(t, err)

	docJSON1, err := docRoot.GetJSON()
	assert.NoError(t, err)
	assert.Equal(t, docJSON1, []byte(`{"a":["b","c","d","f"]}`))
}
