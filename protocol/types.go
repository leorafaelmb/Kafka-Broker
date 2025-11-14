package protocol

import "io"

type ReaderByteReader interface {
	io.Reader
	io.ByteReader
}
