package vmon

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	pb "github.com/can1357/vibemon/sdk/go/internal/pb"
)

// ValueCodec is a portable value serialization format.
type ValueCodec uint8

const (
	// ValueJSON is deterministic RFC 8259 JSON.
	ValueJSON ValueCodec = iota + 1
	// ValueCBOR is deterministic RFC 8949 CBOR.
	ValueCBOR
)

// ValueCompression controls envelope payload compression.
type ValueCompression uint8

const (
	// CompressionNone stores serialized bytes directly.
	CompressionNone ValueCompression = iota
	// CompressionGZIP stores a deterministic gzip stream.
	CompressionGZIP
)

// ArtifactValueLoader retrieves compressed bytes for an artifact-backed envelope.
type ArtifactValueLoader func(*ArtifactReference) ([]byte, error)

// ArtifactReference identifies immutable content by SHA-256 digest.
type ArtifactReference struct{ Digest []byte }

// ValueEnvelope is a portable, checksummed serialized value.
type ValueEnvelope struct{ wire *pb.ValueEnvelope }

// EncodeValue serializes a value and computes its uncompressed SHA-256 checksum.
func EncodeValue(value any, codec ValueCodec, compression ValueCompression) (*ValueEnvelope, error) {
	var raw []byte
	var err error
	var serializer pb.ValueSerializer
	switch codec {
	case ValueJSON:
		raw, err = json.Marshal(value)
		serializer = pb.ValueSerializer_VALUE_SERIALIZER_JSON
	case ValueCBOR:
		mode, modeErr := cbor.CanonicalEncOptions().EncMode()
		if modeErr != nil { return nil, modeErr }
		raw, err = mode.Marshal(value)
		serializer = pb.ValueSerializer_VALUE_SERIALIZER_CBOR
	default:
		return nil, fmt.Errorf("vmon: unsupported value codec %d", codec)
	}
	if err != nil { return nil, fmt.Errorf("vmon: encode value: %w", err) }
	stored := raw
	wireCompression := pb.ValueCompression_VALUE_COMPRESSION_NONE
	if compression == CompressionGZIP {
		var buffer bytes.Buffer
		writer, _ := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
		writer.Header.ModTime = zeroTime
		if _, err = writer.Write(raw); err == nil { err = writer.Close() }
		if err != nil { return nil, fmt.Errorf("vmon: compress value: %w", err) }
		stored = buffer.Bytes()
		wireCompression = pb.ValueCompression_VALUE_COMPRESSION_GZIP
	} else if compression != CompressionNone {
		return nil, fmt.Errorf("vmon: unsupported value compression %d", compression)
	}
	digest := sha256.Sum256(raw)
	return &ValueEnvelope{wire: &pb.ValueEnvelope{SchemaVersion: 1, Serializer: serializer, Compression: wireCompression, Checksum: &pb.Digest{Algorithm: pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256, Value: digest[:]}, UncompressedSizeBytes: uint64(len(raw)), Storage: &pb.ValueEnvelope_InlineData{InlineData: stored}}}, nil
}

var zeroTime = func() (t time.Time) { return }()

// Decode decodes and validates the envelope into destination. Cloudpickle is always rejected.
func (value *ValueEnvelope) Decode(destination any, loader ArtifactValueLoader) error {
	if value == nil || value.wire == nil { return errors.New("vmon: nil value envelope") }
	wire := value.wire
	if wire.Serializer == pb.ValueSerializer_VALUE_SERIALIZER_CLOUDPICKLE { return errors.New("vmon: cloudpickle values are trusted Python-only and unsupported by Go") }
	stored := wire.GetInlineData()
	if stored == nil {
		ref := wire.GetArtifact()
		if ref == nil || ref.Digest == nil { return errors.New("vmon: value envelope has no storage") }
		if loader == nil { return errors.New("vmon: artifact-backed value requires a loader") }
		var err error
		stored, err = loader(&ArtifactReference{Digest: append([]byte(nil), ref.Digest.Value...)})
		if err != nil { return fmt.Errorf("vmon: load value artifact: %w", err) }
	}
	raw := stored
	if wire.Compression == pb.ValueCompression_VALUE_COMPRESSION_GZIP {
		reader, err := gzip.NewReader(bytes.NewReader(stored)); if err != nil { return fmt.Errorf("vmon: decompress value: %w", err) }
		raw, err = io.ReadAll(reader); closeErr := reader.Close(); if err == nil { err = closeErr }
		if err != nil { return fmt.Errorf("vmon: decompress value: %w", err) }
	} else if wire.Compression != pb.ValueCompression_VALUE_COMPRESSION_NONE { return errors.New("vmon: unsupported value compression") }
	if uint64(len(raw)) != wire.UncompressedSizeBytes { return errors.New("vmon: value size mismatch") }
	digest := sha256.Sum256(raw)
	if wire.Checksum == nil || wire.Checksum.Algorithm != pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256 || !bytes.Equal(digest[:], wire.Checksum.Value) { return errors.New("vmon: value checksum mismatch") }
	switch wire.Serializer {
	case pb.ValueSerializer_VALUE_SERIALIZER_JSON:
		return json.Unmarshal(raw, destination)
	case pb.ValueSerializer_VALUE_SERIALIZER_CBOR:
		return cbor.Unmarshal(raw, destination)
	default:
		return errors.New("vmon: unsupported value serializer")
	}
}

func envelopeFromWire(wire *pb.ValueEnvelope) *ValueEnvelope { return &ValueEnvelope{wire: wire} }
