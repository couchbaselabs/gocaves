package servers

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"

	"github.com/couchbase/gocbcore/v10/memd"
)

// maxPacketSize is kv_engine's max_packet_size, the largest packet, header
// included, it reads.
const maxPacketSize = 30 * 1024 * 1024

const headerSize = 24

// invalidFrameEncoding marks a request whose framing extras end inside a
// frame. kv_engine's parseFrameExtras answers it with invalid arguments.
var invalidFrameEncoding = []byte("Invalid encoding in FrameExtras")

// InvalidFrameEncoding is the frame the reader appends to a request's
// unsupported frames, after those decoded before it, when the framing extras
// end inside a frame.
var InvalidFrameEncoding = memd.UnsupportedFrame{Type: 0xff, Data: invalidFrameEncoding}

// IsInvalidFrameEncoding reports whether frame is InvalidFrameEncoding. A
// frame the client sent can carry any type and data, so it is recognised by
// its data's backing array.
func IsInvalidFrameEncoding(frame memd.UnsupportedFrame) bool {
	return len(frame.Data) == len(invalidFrameEncoding) && &frame.Data[0] == &invalidFrameEncoding[0]
}

// DecodeCollectionID decodes the collection id at the front of a key as
// kv_engine's unsigned_leb128<uint32_t>::decode does: at most five bytes,
// the last without the continuation bit, with the bits past 32 dropped. It
// returns the id and the bytes it takes, or false when it finds no stop
// byte.
func DecodeCollectionID(key []byte) (uint32, int, bool) {
	var id uint32
	for n := 0; n < min(len(key), 5); n++ {
		id |= uint32(key[n]&0x7f) << (7 * n)
		if key[n]&0x80 == 0 {
			return id, n + 1, true
		}
	}
	return 0, 0, false
}

// CanonicalCollectionID reports whether an encoding of n bytes of id is the
// shortest, as unsigned_leb128<uint32_t>::is_canonical requires.
func CanonicalCollectionID(id uint32, n int) bool {
	return n == 1 || uint64(id) >= uint64(1)<<(7*(n-1))
}

// decodeCollectionKey splits the collection id from the key of a request. A
// key without a stop byte is all collection id, of the default collection,
// so the bytes of the id stay between the extras and the key, as memd lays
// them out.
func decodeCollectionKey(pak *memd.Packet) {
	id, n, ok := DecodeCollectionID(pak.Key)
	if !ok {
		n = len(pak.Key)
	}
	pak.CollectionID = id
	pak.Key = pak.Key[n:]
}

// packetStream lets a memd.Conn decode a packet the reader has already read
// and checked, and write to the connection. pending holds the packet only
// while it is decoded.
type packetStream struct {
	conn    net.Conn
	pending io.Reader
}

func (s *packetStream) Read(p []byte) (int, error) {
	if s.pending == nil {
		return 0, io.EOF
	}
	return s.pending.Read(p)
}

func (s *packetStream) Write(p []byte) (int, error) {
	return s.conn.Write(p)
}

// checkHeader applies kv_engine's Header::isValid and max_packet_size to a
// header, and returns the length of its framing extras and its body.
func checkHeader(header []byte) (int, int, error) {
	magic := memd.CmdMagic(header[0])
	keyLen := int(binary.BigEndian.Uint16(header[2:]))
	framesLen := 0
	switch magic {
	case 0x08, 0x18:
		framesLen = int(header[2])
		keyLen = int(header[3])
	case 0x80, 0x81, 0x82, 0x83:
	default:
		return 0, 0, fmt.Errorf("invalid packet header: magic 0x%02x", uint8(magic))
	}
	extLen := int(header[4])
	// In 64 bits, so a body length near 4 GiB cannot wrap on a 32-bit build.
	bodyLen := uint64(binary.BigEndian.Uint32(header[8:]))
	if uint64(framesLen+extLen+keyLen) > bodyLen {
		return 0, 0, fmt.Errorf("invalid packet header: framing extras %d, extras %d and key %d exceed body %d", framesLen, extLen, keyLen, bodyLen)
	}
	if headerSize+bodyLen > maxPacketSize {
		return 0, 0, fmt.Errorf("packet size %d exceeds the limit %d", headerSize+bodyLen, maxPacketSize)
	}
	return framesLen, int(bodyLen), nil
}

// parseFrames reads the frames at the start of frames that lie wholly within
// it, as kv_engine's parseFrameExtras does, and returns them in order with the
// length they take. A frame's Type is its id; an id above 0xfe, which an
// escaped id can reach and kv_engine does not define, is 0xff.
func parseFrames(frames []byte) ([]memd.UnsupportedFrame, int) {
	var parsed []memd.UnsupportedFrame
	valid := 0
	for valid < len(frames) {
		offset := valid
		id, size := int(frames[offset]>>4), int(frames[offset]&0x0f)
		offset++
		if id == 0x0f {
			if offset+1 > len(frames) {
				return parsed, valid
			}
			id += int(frames[offset])
			offset++
		}
		if size == 0x0f {
			if offset+1 > len(frames) {
				return parsed, valid
			}
			size += int(frames[offset])
			offset++
		}
		if offset+size > len(frames) {
			return parsed, valid
		}
		frame := memd.UnsupportedFrame{Data: frames[offset : offset+size]}
		// The type of Type is unexported, so it is set through reflection.
		reflect.ValueOf(&frame.Type).Elem().SetUint(uint64(min(id, 0xff)))
		parsed = append(parsed, frame)
		valid = offset + size
	}
	return parsed, valid
}

// readPacket reads one packet from the connection. A header kv_engine
// refuses fails the read before its body is read, and the connection is then
// closed as kv_engine closes it.
func (c *MemdClient) readPacket() (*memd.Packet, int, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, 0, err
	}
	framesLen, bodyLen, err := checkHeader(header)
	if err != nil {
		return nil, 0, err
	}
	if handler := c.parent.handlers.PacketSizeHandler; handler != nil && !handler(c, headerSize+bodyLen) {
		return nil, 0, fmt.Errorf("packet size %d refused", headerSize+bodyLen)
	}
	frames := make([]byte, framesLen)
	if _, err := io.ReadFull(c.conn, frames); err != nil {
		return nil, 0, err
	}

	// memd cannot decode a frame that runs past the framing extras, so the
	// request is decoded with the frames before it only.
	parsed, validLen := parseFrames(frames)
	if validLen < framesLen {
		header[2] = byte(validLen)
		binary.BigEndian.PutUint32(header[8:], uint32(bodyLen-(framesLen-validLen)))
	}

	// memd reads the rest of the body from the connection into its own
	// buffer.
	c.stream.pending = io.MultiReader(bytes.NewReader(header), bytes.NewReader(frames[:validLen]), io.LimitReader(c.conn, int64(bodyLen-framesLen)))
	defer func() { c.stream.pending = nil }()
	// The reader decodes the collection id of a key itself, as memd refuses
	// encodings kv_engine reads.
	pak, _, err := c.rconn.ReadPacket()
	if err != nil {
		return nil, 0, err
	}
	if n, _ := c.stream.pending.Read(make([]byte, 1)); n != 0 {
		return nil, 0, errors.New("packet decoded short of its body")
	}
	// memd decodes frames into fields that keep neither their order nor ids
	// above 0xff, so every frame is listed here for validation.
	pak.UnsupportedFrames = parsed
	if validLen < framesLen {
		pak.UnsupportedFrames = append(pak.UnsupportedFrames, InvalidFrameEncoding)
	}
	if c.mconn.IsFeatureEnabled(memd.FeatureCollections) {
		if pak.Command == memd.CmdObserve {
			return nil, 0, errors.New("the observe operation is not supported with collections enabled")
		}
		if len(pak.Key) > 0 && memd.IsCommandCollectionEncoded(pak.Command) {
			decodeCollectionKey(pak)
		}
	}
	return pak, headerSize + bodyLen, nil
}
