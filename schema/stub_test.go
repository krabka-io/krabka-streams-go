package schema

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

// registryStub is a per-request scripted HTTP server, the test double for a
// schema registry when the HTTP interaction itself is what is under test.
type registryStub struct {
	server *httptest.Server

	mu      sync.Mutex
	replies map[string]stubReply
	bodies  map[string]string
	counts  map[string]int
}

type stubReply struct {
	status int
	body   string
}

func newRegistryStub() *registryStub {
	stub := &registryStub{
		replies: map[string]stubReply{},
		bodies:  map[string]string{},
		counts:  map[string]int{},
	}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	return stub
}

func (s *registryStub) url() string { return s.server.URL }

func (s *registryStub) close() { s.server.Close() }

func (s *registryStub) reply(method, path string, status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[method+" "+path] = stubReply{status: status, body: body}
}

func (s *registryStub) body(method, path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bodies[method+" "+path]
}

func (s *registryStub) count(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[method+" "+path]
}

func (s *registryStub) handle(writer http.ResponseWriter, request *http.Request) {
	key := request.Method + " " + request.URL.Path
	requestBody, _ := io.ReadAll(request.Body)
	s.mu.Lock()
	s.bodies[key] = string(requestBody)
	s.counts[key]++
	reply, ok := s.replies[key]
	s.mu.Unlock()
	if !ok {
		reply = stubReply{status: 404, body: "not found"}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(reply.status)
	_, _ = writer.Write([]byte(reply.body))
}
