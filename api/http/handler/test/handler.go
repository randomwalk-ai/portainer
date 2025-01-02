package test

import (
	"net/http"

	"github.com/portainer/portainer/api/dataservices"
	"github.com/portainer/portainer/api/http/security"
	httperror "github.com/portainer/portainer/pkg/libhttp/error"
	"github.com/gorilla/mux"
)

type Handler struct {
	*mux.Router
	DataStore dataservices.DataStore
}

func NewHandler(bouncer security.BouncerService, dataStore dataservices.DataStore) *Handler {
	h := &Handler{
        Router:    mux.NewRouter(),
        DataStore: dataStore,
    }

    h.Handle("/hello",
		bouncer.PublicAccess(httperror.LoggerHandler(h.hello))).Methods(http.MethodGet)
    h.Handle("/create/table",
		bouncer.AdminAccess(httperror.LoggerHandler(h.createTable))).Methods(http.MethodPost)

    return h
}

// func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	fmt.Println("Received request: Method=%s, Path=%s", r.Method, r.URL.Path)
	
// 	switch r.Method {
// 	case http.MethodGet:
// 		h.hello(w, r)
// 	default:
// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 	}
// }
