package servers

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

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

// validFramesLen returns the length of the frames at the start of frames that
// lie wholly within it, reading them as kv_engine's parseFrameExtras does.
func validFramesLen(frames []byte) int {
	valid := 0
	for valid < len(frames) {
		offset := valid
		id, size := frames[offset]>>4, int(frames[offset]&0x0f)
		offset++
		if id == 0x0f {
			if offset+1 > len(frames) {
				return valid
			}
			offset++
		}
		if size == 0x0f {
			if offset+1 > len(frames) {
				return valid
			}
			size += int(frames[offset])
			offset++
		}
		if offset+size > len(frames) {
			return valid
		}
		valid = offset + size
	}
	return valid
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
	validLen := validFramesLen(frames)
	if validLen < framesLen {
		header[2] = byte(validLen)
		binary.BigEndian.PutUint32(header[8:], uint32(bodyLen-(framesLen-validLen)))
	}

	// memd reads the rest of the body from the connection into its own
	// buffer.
	c.stream.pending = io.MultiReader(bytes.NewReader(header), bytes.NewReader(frames[:validLen]), io.LimitReader(c.conn, int64(bodyLen-framesLen)))
	defer func() { c.stream.pending = nil }()
	pak, _, err := c.mconn.ReadPacket()
	if err != nil {
		return nil, 0, err
	}
	if n, _ := c.stream.pending.Read(make([]byte, 1)); n != 0 {
		return nil, 0, errors.New("packet decoded short of its body")
	}
	if validLen < framesLen {
		pak.UnsupportedFrames = append(pak.UnsupportedFrames, InvalidFrameEncoding)
	}
	return pak, headerSize + bodyLen, nil
}
