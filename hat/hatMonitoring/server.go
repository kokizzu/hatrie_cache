package hatMonitoring

import "net/http"

// Server owns monitoring HTTP route composition independently from cache
// state. Applications provide their own endpoint handlers and middleware.
type Server struct {
	mux *http.ServeMux
}

// NewServer creates an empty monitoring route server.
func NewServer() *Server {
	return &Server{mux: http.NewServeMux()}
}

// Handle registers one monitoring route.
func (server *Server) Handle(pattern string, handler http.Handler) {
	server.mux.Handle(pattern, handler)
}

// HandleFunc registers one monitoring route function.
func (server *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	server.mux.HandleFunc(pattern, handler)
}

// Handler returns the composed route handler.
func (server *Server) Handler() http.Handler {
	if server == nil || server.mux == nil {
		return http.NotFoundHandler()
	}
	return server.mux
}
