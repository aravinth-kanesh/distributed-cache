package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

// I designed a compact binary format for snapshots. Fixed-size integer
// encoding (big-endian) is simpler and faster to parse than varints,
// and 4GB max per value is more than sufficient for a cache.

var (
	snapshotMagic    = []byte("DCACHE")
	snapshotVersion  = uint16(1)
	byteOrder        = binary.BigEndian
	crc32cTable      = crc32.MakeTable(crc32.Castagnoli)

	ErrBadMagic      = errors.New("snapshot: invalid magic bytes")
	ErrBadVersion    = errors.New("snapshot: unsupported format version")
	ErrBadChecksum   = errors.New("snapshot: CRC-32C checksum mismatch")
	ErrCorrupt       = errors.New("snapshot: corrupt data")
)

// Snapshot entry type markers
const (
	SnapTypeString byte = 0x00
	SnapTypeList   byte = 0x01
	SnapTypeHash   byte = 0x02
	SnapTypeSet    byte = 0x03
	SnapTypeEOF    byte = 0xFF
)

// SnapshotEncoder writes entries in the binary snapshot format.
// All writes pass through a CRC-32C hasher for integrity verification.
type SnapshotEncoder struct {
	w   io.Writer
	crc hash.Hash32
	buf [8]byte // Scratch buffer for integer encoding
}

// NewSnapshotEncoder creates a new encoder wrapping the given writer.
func NewSnapshotEncoder(w io.Writer) *SnapshotEncoder {
	crc := crc32.New(crc32cTable)
	return &SnapshotEncoder{
		w:   io.MultiWriter(w, crc),
		crc: crc,
	}
}

// WriteHeader writes the file header: magic, version, timestamp, key count.
func (e *SnapshotEncoder) WriteHeader(timestamp int64, keyCount int64) error {
	if _, err := e.w.Write(snapshotMagic); err != nil {
		return err
	}
	if err := e.writeUint16(snapshotVersion); err != nil {
		return err
	}
	if err := e.writeInt64(timestamp); err != nil {
		return err
	}
	return e.writeInt64(keyCount)
}

// WriteStringEntry writes a complete string-type entry.
func (e *SnapshotEncoder) WriteStringEntry(key string, expiresAt int64, value []byte) error {
	if err := e.writeEntryHeader(SnapTypeString, key, expiresAt); err != nil {
		return err
	}
	return e.writeBytes(value)
}

// WriteListEntry writes a complete list-type entry.
// Elements must be provided in head-to-tail order.
func (e *SnapshotEncoder) WriteListEntry(key string, expiresAt int64, elements [][]byte) error {
	if err := e.writeEntryHeader(SnapTypeList, key, expiresAt); err != nil {
		return err
	}
	if err := e.writeUint32(uint32(len(elements))); err != nil {
		return err
	}
	for _, elem := range elements {
		if err := e.writeBytes(elem); err != nil {
			return err
		}
	}
	return nil
}

// WriteHashEntry writes a complete hash-type entry.
func (e *SnapshotEncoder) WriteHashEntry(key string, expiresAt int64, fields map[string][]byte) error {
	if err := e.writeEntryHeader(SnapTypeHash, key, expiresAt); err != nil {
		return err
	}
	if err := e.writeUint32(uint32(len(fields))); err != nil {
		return err
	}
	for field, val := range fields {
		if err := e.writeBytes([]byte(field)); err != nil {
			return err
		}
		if err := e.writeBytes(val); err != nil {
			return err
		}
	}
	return nil
}

// WriteSetEntry writes a complete set-type entry.
func (e *SnapshotEncoder) WriteSetEntry(key string, expiresAt int64, members map[string]struct{}) error {
	if err := e.writeEntryHeader(SnapTypeSet, key, expiresAt); err != nil {
		return err
	}
	if err := e.writeUint32(uint32(len(members))); err != nil {
		return err
	}
	for member := range members {
		if err := e.writeBytes([]byte(member)); err != nil {
			return err
		}
	}
	return nil
}

// WriteFooter writes the EOF marker and CRC-32C checksum.
// The checksum covers all preceding bytes.
func (e *SnapshotEncoder) WriteFooter() error {
	// Write EOF marker through the CRC writer
	if _, err := e.w.Write([]byte{SnapTypeEOF}); err != nil {
		return err
	}
	// Write checksum directly to the underlying writer (not through CRC)
	// We need the raw writer, so we extract it from the MultiWriter.
	// Since we constructed with io.MultiWriter(w, crc), the checksum
	// at this point covers everything including the EOF byte.
	checksum := e.crc.Sum32()
	byteOrder.PutUint32(e.buf[:4], checksum)

	// The footer's CRC bytes must go to the underlying writer only,
	// not back through the CRC hasher. I stored the original writer
	// as the first in the MultiWriter, but since we can't extract it,
	// I'll write the CRC through the multi-writer — it doesn't matter
	// because we've already captured the checksum value.
	_, err := e.w.Write(e.buf[:4])
	return err
}

// writeEntryHeader writes: type (1B) | expiresAt (8B) | keyLen (4B) | key
func (e *SnapshotEncoder) writeEntryHeader(entryType byte, key string, expiresAt int64) error {
	if _, err := e.w.Write([]byte{entryType}); err != nil {
		return err
	}
	if err := e.writeInt64(expiresAt); err != nil {
		return err
	}
	return e.writeBytes([]byte(key))
}

func (e *SnapshotEncoder) writeBytes(b []byte) error {
	if err := e.writeUint32(uint32(len(b))); err != nil {
		return err
	}
	_, err := e.w.Write(b)
	return err
}

func (e *SnapshotEncoder) writeUint16(v uint16) error {
	byteOrder.PutUint16(e.buf[:2], v)
	_, err := e.w.Write(e.buf[:2])
	return err
}

func (e *SnapshotEncoder) writeUint32(v uint32) error {
	byteOrder.PutUint32(e.buf[:4], v)
	_, err := e.w.Write(e.buf[:4])
	return err
}

func (e *SnapshotEncoder) writeInt64(v int64) error {
	byteOrder.PutUint64(e.buf[:8], uint64(v))
	_, err := e.w.Write(e.buf[:8])
	return err
}

// SnapshotDecoder reads entries from the binary snapshot format.
type SnapshotDecoder struct {
	r   io.Reader
	crc hash.Hash32
	buf [8]byte
}

// NewSnapshotDecoder creates a new decoder wrapping the given reader.
func NewSnapshotDecoder(r io.Reader) *SnapshotDecoder {
	crc := crc32.New(crc32cTable)
	return &SnapshotDecoder{
		r:   io.TeeReader(r, crc),
		crc: crc,
	}
}

// ReadHeader reads and validates the file header.
// Returns the snapshot timestamp and expected key count.
func (d *SnapshotDecoder) ReadHeader() (timestamp int64, keyCount int64, err error) {
	magic := make([]byte, 6)
	if _, err = io.ReadFull(d.r, magic); err != nil {
		return 0, 0, fmt.Errorf("reading magic: %w", err)
	}
	if string(magic) != string(snapshotMagic) {
		return 0, 0, ErrBadMagic
	}

	version, err := d.readUint16()
	if err != nil {
		return 0, 0, fmt.Errorf("reading version: %w", err)
	}
	if version != snapshotVersion {
		return 0, 0, fmt.Errorf("%w: got %d, expected %d", ErrBadVersion, version, snapshotVersion)
	}

	timestamp, err = d.readInt64()
	if err != nil {
		return 0, 0, fmt.Errorf("reading timestamp: %w", err)
	}

	keyCount, err = d.readInt64()
	if err != nil {
		return 0, 0, fmt.Errorf("reading key count: %w", err)
	}

	return timestamp, keyCount, nil
}

// ReadEntry reads the next entry from the snapshot.
// Returns the entry type, key, expiresAt, and type-specific data.
// When the EOF marker is reached, entryType will be SnapTypeEOF.
//
// Return values by type:
//   - SnapTypeString: data is []byte
//   - SnapTypeList:   data is [][]byte (elements in order)
//   - SnapTypeHash:   data is map[string][]byte
//   - SnapTypeSet:    data is map[string]struct{}
//   - SnapTypeEOF:    data is nil
func (d *SnapshotDecoder) ReadEntry() (entryType byte, key string, expiresAt int64, data interface{}, err error) {
	var typeBuf [1]byte
	if _, err = io.ReadFull(d.r, typeBuf[:]); err != nil {
		return 0, "", 0, nil, err
	}
	entryType = typeBuf[0]

	if entryType == SnapTypeEOF {
		return SnapTypeEOF, "", 0, nil, nil
	}

	expiresAt, err = d.readInt64()
	if err != nil {
		return 0, "", 0, nil, fmt.Errorf("reading expiresAt: %w", err)
	}

	keyBytes, err := d.readBytes()
	if err != nil {
		return 0, "", 0, nil, fmt.Errorf("reading key: %w", err)
	}
	key = string(keyBytes)

	switch entryType {
	case SnapTypeString:
		val, err := d.readBytes()
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("reading string value: %w", err)
		}
		return entryType, key, expiresAt, val, nil

	case SnapTypeList:
		count, err := d.readUint32()
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("reading list count: %w", err)
		}
		elements := make([][]byte, count)
		for i := uint32(0); i < count; i++ {
			elem, err := d.readBytes()
			if err != nil {
				return 0, "", 0, nil, fmt.Errorf("reading list element %d: %w", i, err)
			}
			elements[i] = elem
		}
		return entryType, key, expiresAt, elements, nil

	case SnapTypeHash:
		count, err := d.readUint32()
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("reading hash count: %w", err)
		}
		fields := make(map[string][]byte, count)
		for i := uint32(0); i < count; i++ {
			field, err := d.readBytes()
			if err != nil {
				return 0, "", 0, nil, fmt.Errorf("reading hash field %d: %w", i, err)
			}
			val, err := d.readBytes()
			if err != nil {
				return 0, "", 0, nil, fmt.Errorf("reading hash value %d: %w", i, err)
			}
			fields[string(field)] = val
		}
		return entryType, key, expiresAt, fields, nil

	case SnapTypeSet:
		count, err := d.readUint32()
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("reading set count: %w", err)
		}
		members := make(map[string]struct{}, count)
		for i := uint32(0); i < count; i++ {
			member, err := d.readBytes()
			if err != nil {
				return 0, "", 0, nil, fmt.Errorf("reading set member %d: %w", i, err)
			}
			members[string(member)] = struct{}{}
		}
		return entryType, key, expiresAt, members, nil

	default:
		return 0, "", 0, nil, fmt.Errorf("%w: unknown entry type 0x%02x", ErrCorrupt, entryType)
	}
}

// VerifyChecksum reads the trailing CRC-32C and validates it against
// the running checksum. Must be called after ReadEntry returns SnapTypeEOF.
func (d *SnapshotDecoder) VerifyChecksum() error {
	// The CRC in the file was written after the EOF marker. At this point
	// the TeeReader has fed everything up to here (including the EOF byte)
	// into our CRC hasher. Now I read the 4-byte checksum directly from
	// the underlying reader (bypassing the TeeReader) and compare.
	expected := d.crc.Sum32()

	// Read the checksum bytes — these go through TeeReader but that's fine,
	// I already captured the expected value above.
	stored, err := d.readUint32()
	if err != nil {
		return fmt.Errorf("reading checksum: %w", err)
	}

	if stored != expected {
		return fmt.Errorf("%w: stored=0x%08x computed=0x%08x", ErrBadChecksum, stored, expected)
	}
	return nil
}

func (d *SnapshotDecoder) readBytes() ([]byte, error) {
	length, err := d.readUint32()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(d.r, buf)
	return buf, err
}

func (d *SnapshotDecoder) readUint16() (uint16, error) {
	if _, err := io.ReadFull(d.r, d.buf[:2]); err != nil {
		return 0, err
	}
	return byteOrder.Uint16(d.buf[:2]), nil
}

func (d *SnapshotDecoder) readUint32() (uint32, error) {
	if _, err := io.ReadFull(d.r, d.buf[:4]); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(d.buf[:4]), nil
}

func (d *SnapshotDecoder) readInt64() (int64, error) {
	if _, err := io.ReadFull(d.r, d.buf[:8]); err != nil {
		return 0, err
	}
	return int64(byteOrder.Uint64(d.buf[:8])), nil
}
