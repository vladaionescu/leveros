package http2stream

import (
	"strings"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const (
	// http://http2.github.io/http2-spec/#SettingValues
	http2InitHeaderTableSize = 4096
)

// headerFrame is either a http2.HeaderFrame or http2.ContinuationFrame.
type headerFrame interface {
	Header() http2.FrameHeader
	HeaderBlockFragment() []byte
	HeadersEnded() bool
}

type hpackDecoder struct {
	decoder *hpack.Decoder
	err     error
	headers map[string][]string
}

func newHPACKDecoder() *hpackDecoder {
	hpackDec := &hpackDecoder{}
	hpackDec.reset()
	hpackDec.decoder = hpack.NewDecoder(
		http2InitHeaderTableSize, func(field hpack.HeaderField) {
			key := strings.ToLower(field.Name)
			hpackDec.headers[key] = append(
				hpackDec.headers[key], field.Value)
		})
	return hpackDec
}

func (hpackDec *hpackDecoder) reset() {
	hpackDec.headers = make(map[string][]string)
	hpackDec.err = nil
}

func (hpackDec *hpackDecoder) decode(
	frame headerFrame) (endHeaders bool, err error) {
	_, hpackDec.err = hpackDec.decoder.Write(frame.HeaderBlockFragment())

	if frame.HeadersEnded() {
		closeErr := hpackDec.decoder.Close()
		if closeErr != nil && hpackDec.err == nil {
			hpackDec.err = closeErr
		}
		endHeaders = true
	}
	return endHeaders, hpackDec.err
}
