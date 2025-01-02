package test

import (
	"net/http"
	"errors"
	"github.com/portainer/portainer/pkg/libhttp/response"
	httperror "github.com/portainer/portainer/pkg/libhttp/error"
	"github.com/portainer/portainer/pkg/libhttp/request"
)

type tableCreatePayload struct {
	Name string
}

func (payload *tableCreatePayload) Validate(r *http.Request) error {
	if len(payload.Name) == 0 {
		return errors.New("invalid table name")
	}

	return nil
}

func (h *Handler) hello(w http.ResponseWriter, r *http.Request) *httperror.HandlerError {
	// Structured response matching the OpenAPI specification
	responseData := map[string]string{
		"message": "Hello, from portainer!",
	}

	// Use the standard response helper
	return response.JSON(w, responseData)
}

func (h *Handler) createTable(w http.ResponseWriter, r *http.Request) *httperror.HandlerError {
	// Decode the payload
	var payload tableCreatePayload
	err := request.DecodeAndValidateJSONPayload(r, &payload)
	if err != nil {
		return httperror.BadRequest("Invalid request payload", err)
	}

	// Start a database transaction
	connection := h.DataStore.Connection()

	err = connection.SetServiceName(payload.Name)
	if err != nil {
		return httperror.InternalServerError("Failed to create table", err)
	}

	// Send success response
	responseData := map[string]string{
		"message": "Table '" + payload.Name + "' created successfully!",
	}
	return response.JSON(w, responseData)
}