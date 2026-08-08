package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"api/internal/platform"
)

// OrderCreated implements platform.Event: it names its own topic and partition
// key, so PostOrder never has to.
type OrderCreated struct {
	OrderID  string  `json:"orderId"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

func (o OrderCreated) Topic() platform.Topic { return platform.TopicOrderCreated }

// Key partitions by order so every event of one order keeps its ordering.
func (o OrderCreated) Key() *string { return &o.OrderID }

func (h *Handler) PostOrder(w http.ResponseWriter, r *http.Request) {
	var order OrderCreated

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if order.OrderID == "" {
		writeError(w, http.StatusBadRequest, "orderId is required")
		return
	}

	if err := h.publisher.Publish(r.Context(), order); err != nil {
		slog.Error("failed to publish order", "error", err, "orderId", order.OrderID)
		writeError(w, http.StatusBadGateway, "failed to publish order")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"orderId": order.OrderID})
}
