package gateway

import (
	"encoding/json"
	"net/http"

	"api/internal/platform"
)

// Handler carries one publisher for every endpoint. Each endpoint publishes an
// event that declares its own topic, so no per-topic wiring is needed here.
type Handler struct {
	publisher *platform.KafkaPublisher
}

func NewHandler(publisher *platform.KafkaPublisher) *Handler {
	return &Handler{
		publisher: publisher,
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
