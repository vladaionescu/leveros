package apiserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/host"
	"github.com/leveros/leveros/leverutil"
)

// PackageName is the name of this package.
const PackageName = "apiserver"

var (
	// ListenPortFlag is the port the API server should listen on.
	ListenPortFlag = config.DeclareString(PackageName, "listenPort", "3503")
	// DisableAPIServerFlag disables the creation of an HTTP API server.
	DisableAPIServerFlag = config.DeclareBool(PackageName, "disableAPIServer")
)

var logger = leverutil.GetLogger(PackageName, "Server")

const maxNonChanRequestSize = 512 * 1024 // 512 KiB

var bufferedReaderPool = &sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, maxNonChanRequestSize)
	},
}

var bufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, maxNonChanRequestSize)
	},
}

// Server is an HTTP API server that acts as a gateway to Lever services for
// non-gRPC clients.
type Server struct {
	httpServer  *http.Server
	leverClient *leverapi.Client
}

// NewServer returns a new instance of Server.
func NewServer() (server *Server, err error) {
	if DisableAPIServerFlag.Get() {
		return nil, nil
	}
	server = new(Server)
	server.leverClient, err = leverapi.NewClient()
	if err != nil {
		return nil, err
	}
	// Pipe Lever calls into own host listener.
	server.leverClient.ForceHost =
		"127.0.0.1:" + host.EnvExtListenPortFlag.Get()
	server.httpServer = &http.Server{
		Addr:    ":" + ListenPortFlag.Get(),
		Handler: server,
	}
	go server.listenAndServe()
	return server, nil
}

func (server *Server) listenAndServe() {
	err := server.httpServer.ListenAndServe()
	if err != nil {
		logger.WithFields("err", err).Error("Listen and server error")
	}
}

// ServeHTTP serves individual HTTP requests.
func (server *Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	origin := req.Header.Get("Origin")
	if origin != "" {
		// TODO: Perhaps a lever.json config param could restrict origin
		//       to prevent CSRF.
		resp.Header().Set("Access-Control-Allow-Origin", origin)
		resp.Header().Set(
			"Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, "+
				"X-CSRF-Token, Authorization")
	}
	if req.Method == "OPTIONS" {
		resp.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		resp.WriteHeader(http.StatusOK)
		return
	}
	if req.Method != "POST" {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	leverURLStr := fmt.Sprintf("lever://%s%s", req.Host, req.URL.Path)
	leverURL, err := core.ParseLeverURL(leverURLStr)
	if err != nil {
		logger.WithFields("err", err).Debug("Error parsing Lever URL")
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	queryValues, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		logger.WithFields("err", err).Debug("Error parsing URL query")
		resp.WriteHeader(http.StatusBadRequest)
		return
	}
	forceEnv := queryValues.Get("forceenv")
	if forceEnv != "" {
		leverURL.Environment = forceEnv
	}
	leverURLStr = leverURL.String()

	reader := bufferedReaderPool.Get().(*bufio.Reader)
	reader.Reset(req.Body)
	defer bufferedReaderPool.Put(reader)
	defer req.Body.Close()
	if leverapi.IsChanMethod(leverURL.Method) {
		// TODO: Byte args not supported. Any way to support that?
		//       How to delimit args from rest?
		done := false
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				done = true
			} else {
				logger.WithFields("err", err).Error("Read error")
				return
			}
		}
		if len(line) == 0 {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}
		var args []interface{}
		err = json.Unmarshal(line, &args)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			logger.WithFields("err", err).Debug("Malformed JSON")
			return
		}
		var stream leverapi.Stream
		stream, err = server.leverClient.InvokeChanURL(leverURLStr, args...)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			logger.WithFields("err", err).Error("InvokeChanURL error")
			return
		}
		errCh := make(chan bool)
		workerDoneCh := make(chan struct{})
		go replyStreamWorker(stream, resp, errCh, workerDoneCh)
		if req.Header.Get("Content-Type") == "application/json" {
			for !done {
				line, err = reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						done = true
					} else {
						logger.WithFields("err", err).Error("Read error")
						errCh <- true
						<-workerDoneCh
						return
					}
				}
				if len(line) > 0 {
					var msg interface{}
					err = json.Unmarshal(line, &msg)
					if err != nil {
						logger.WithFields("err", err).Debug("Malformed JSON")
						errCh <- true
						<-workerDoneCh
						return
					}
					stream.Send(msg)
				}
			}
		} else {
			for !done {
				buffer := bufferPool.Get().([]byte)
				defer bufferPool.Put(buffer)
				var size int
				size, err = reader.Read(buffer)
				if err != nil {
					if err == io.EOF {
						done = true
					}
					logger.WithFields("err", err).Error("Read error")
					errCh <- true
					<-workerDoneCh
					return
				}
				msg := buffer[:size]
				stream.Send(msg)
			}
		}
		err = stream.Close()
		if err != nil {
			logger.WithFields("err", err).Debug(
				"Stream close error")
			errCh <- true
			<-workerDoneCh
			return
		}
		errCh <- false
		<-workerDoneCh
	} else {
		buffer := bufferPool.Get().([]byte)
		defer bufferPool.Put(buffer)
		var size int
		size, err = io.ReadFull(reader, buffer)
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				logger.WithFields("err", err).Error("Read error")
				return
			}
		}
		if size == maxNonChanRequestSize &&
			err != io.EOF &&
			err != io.ErrUnexpectedEOF {
			resp.WriteHeader(http.StatusBadRequest)
			_, err = resp.Write([]byte("\"Exceeded maximum request size\""))
			if err != nil {
				logger.WithFields("err", err).Debug("Write error")
			}
			return
		}
		var args []interface{}
		contentType := req.Header.Get("Content-Type")
		contentTypeSplit := strings.Split(contentType, ";")
		switch contentTypeSplit[0] {
		case "application/json":
			err = json.Unmarshal(buffer[:size], &args)
			if err != nil {
				resp.WriteHeader(http.StatusBadRequest)
				logger.WithFields("err", err).Debug("JSON unmarshal error")
				return
			}
		case "application/x-www-form-urlencoded":
			// TODO
			resp.WriteHeader(http.StatusBadRequest)
			logger.WithFields("contentType", contentType).Error(
				"Content type not yet supported")
			return
		default:
			args = make([]interface{}, 1)
			args[0] = buffer[:size]
		}
		var reply interface{}
		err = server.leverClient.InvokeURL(&reply, leverURLStr, args...)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			remoteErr, ok := err.(*leverapi.RemoteError)
			if ok {
				reply = remoteErr.Err
			} else {
				remoteByteErr, ok := err.(*leverapi.RemoteByteError)
				if ok {
					reply = remoteByteErr.Err
				} else {
					logger.WithFields("err", err).Error("Internal Lever error")
					return
				}
			}
		} else {
			resp.WriteHeader(http.StatusOK)
		}

		err = writeReply(resp, reply)
		if err != nil {
			logger.WithFields("err", err).Error("Writing reply failed")
			return
		}
	}
}

func replyStreamWorker(
	stream leverapi.Stream, resp http.ResponseWriter, errCh chan bool,
	workerDoneCh chan struct{}) {
	isFirst := true
	defer close(workerDoneCh)
	receiveCh := make(chan interface{})
	receiveErrCh := make(chan error)
	go streamReceiveWorker(stream, receiveCh, receiveErrCh)
	for {
		select {
		case msg := <-receiveCh:
			if isFirst {
				resp.WriteHeader(http.StatusOK)
				isFirst = false
			}
			byteMsg, isByte := msg.([]byte)
			if isByte {
				if isFirst {
					resp.Header().Set("Content-Type", "application/json")
				}
				_, err := resp.Write(byteMsg)
				if err != nil {
					logger.WithFields("err", err).Debug("Write error")
					return
				}
			} else {
				if isFirst {
					resp.Header().Set(
						"Content-Type", "application/octet-stream")
				}
				var err error
				byteMsg, err = json.Marshal(msg)
				if err != nil {
					logger.WithFields("err", err).Error("JSON marshal error")
					return
				}
				_, err = resp.Write(byteMsg)
				if err != nil {
					logger.WithFields("err", err).Debug("Write error")
					return
				}
			}
		case err := <-receiveErrCh:
			if err == io.EOF {
				if isFirst {
					resp.WriteHeader(http.StatusOK)
				}
				return
			}
			if isFirst {
				resp.WriteHeader(http.StatusInternalServerError)
			}
			var byteErr []byte
			remoteErr, ok := err.(*leverapi.RemoteError)
			if ok {
				if isFirst {
					resp.Header().Set("Content-Type", "application/json")
				}
				byteErr, err = json.Marshal(remoteErr.Err)
				if err != nil {
					logger.WithFields("err", err).Error("JSON marshal error")
					return
				}
			} else {
				remoteByteErr, ok := err.(*leverapi.RemoteByteError)
				if ok {
					if isFirst {
						resp.Header().Set(
							"Content-Type", "application/octet-stream")
					}
					byteErr = remoteByteErr.Err
				} else {
					logger.WithFields("err", err).Error("Internal Lever error")
					return
				}
			}
			_, err = resp.Write(byteErr)
			if err != nil {
				logger.WithFields("err", err).Debug("Write error")
				return
			}
		case isErr := <-errCh:
			if isErr && isFirst {
				resp.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		isFirst = false
	}
}

func streamReceiveWorker(
	stream leverapi.Stream, receiveCh chan interface{},
	receiveErrCh chan error) {
	for {
		var msg interface{}
		err := stream.Receive(&msg)
		if err != nil {
			receiveErrCh <- err
			return
		}
		receiveCh <- msg
	}
}

func writeReply(resp http.ResponseWriter, reply interface{}) (err error) {
	bytes, isByte := reply.([]byte)
	if !isByte {
		resp.Header().Set("Content-Type", "application/json")
		bytes, err = json.Marshal(reply)
		if err != nil {
			return err
		}
	} else {
		resp.Header().Set("Content-Type", "application/octet-stream")
	}
	_, err = resp.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
