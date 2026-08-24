package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"api/internal/platform"
)

// OrderItem mirrors com.github.overz.shared.event.OrderItem.
//
// unitPrice is a json.Number, not a float64: the Java side reads money into
// BigDecimal and TotalAmountMatchesItemsRule compares the declared total
// against the sum of the items at scale 2. Going through a float would let
// 0.1 + 0.2 arrive as 0.30000000000000004 and reject a perfectly good order.
// json.Number keeps whatever decimal the client sent, byte for byte.
type OrderItem struct {
	ProductID string      `json:"productId,omitempty"`
	Quantity  *int        `json:"quantity,omitempty"`
	UnitPrice json.Number `json:"unitPrice,omitempty"`
}

// OrderCreated mirrors com.github.overz.shared.event.OrderCreated field for
// field. It has to: orders/ deserializes this exact record, and anything that
// does not match its shape is rejected by OrderValidationRules and lands in
// orders.dlq instead of flowing through the pipeline.
//
// Every field is omitempty and nothing but orderId is required here. Validation
// belongs to the Streams app by design -- a missing currency has to reach
// orders.created and come back out of the DLQ with CURRENCY_REQUIRED, which is
// the behaviour the lab exists to show. Rejecting it at the edge would hide it.
type OrderCreated struct {
	OrderID     string      `json:"orderId"`
	CustomerID  string      `json:"customerId,omitempty"`
	Items       []OrderItem `json:"items,omitempty"`
	TotalAmount json.Number `json:"totalAmount,omitempty"`
	Currency    string      `json:"currency,omitempty"`
	CreatedAt   *time.Time  `json:"createdAt,omitempty"`
}

func (o OrderCreated) Topic() platform.Topic { return platform.TopicOrderCreated }

// Key partitions by order so every event of one order keeps its ordering.
func (o OrderCreated) Key() *string { return &o.OrderID }

func (h *Handler) PostOrder(w http.ResponseWriter, r *http.Request) {
	var order OrderCreated

	decoder := json.NewDecoder(r.Body)
	// Without UseNumber the decoder turns every number into float64 and the
	// json.Number fields above would come back empty.
	decoder.UseNumber()

	if err := decoder.Decode(&order); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// orderId is the partition key, so an empty one is not a domain problem the
	// DLQ can explain -- it would scatter one order across partitions and lose
	// its ordering. That one is checked here.
	if order.OrderID == "" {
		writeError(w, http.StatusBadRequest, "orderId is required")
		return
	}

	// The moment the order entered the system. The client may pin it (replaying
	// a fixture, say); otherwise the edge stamps it, because "when was this
	// created" is not something the Streams app can recover later.
	if order.CreatedAt == nil {
		now := time.Now().UTC()
		order.CreatedAt = &now
	}

	if err := h.publisher.Publish(r.Context(), order); err != nil {
		slog.Error("failed to publish order", "error", err, "orderId", order.OrderID)
		writeError(w, http.StatusBadGateway, "failed to publish order")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"orderId": order.OrderID})
}
