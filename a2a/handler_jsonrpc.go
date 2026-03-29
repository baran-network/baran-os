package a2a

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

// JSONRPCHandler dispatches A2A JSON-RPC 2.0 requests.
type JSONRPCHandler struct {
	tasks  *TaskManager
	logger *slog.Logger
}

// NewJSONRPCHandler creates a JSONRPCHandler.
func NewJSONRPCHandler(tasks *TaskManager, logger *slog.Logger) *JSONRPCHandler {
	return &JSONRPCHandler{tasks: tasks, logger: logger}
}

// HandleJSONRPC serves POST / for A2A JSON-RPC operations.
func (h *JSONRPCHandler) HandleJSONRPC(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONRPC(w, nil, &JSONRPCError{Code: ErrCodeParseError, Message: "failed to read request body"})
		return
	}

	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONRPC(w, nil, &JSONRPCError{Code: ErrCodeParseError, Message: "invalid JSON"})
		return
	}

	if req.JSONRPC != "2.0" {
		writeJSONRPC(w, req.ID, &JSONRPCError{Code: ErrCodeInvalidRequest, Message: "jsonrpc must be \"2.0\""})
		return
	}

	switch req.Method {
	case "message/send":
		h.handleMessageSend(w, r, req)
	case "tasks/get":
		h.handleTasksGet(w, r, req)
	case "tasks/cancel":
		h.handleTasksCancel(w, r, req)
	default:
		writeJSONRPC(w, req.ID, &JSONRPCError{Code: ErrCodeMethodNotFound, Message: "method not found: " + req.Method})
	}
}

func (h *JSONRPCHandler) handleMessageSend(w http.ResponseWriter, r *http.Request, req JSONRPCRequest) {
	params, err := unmarshalParams[SendMessageParams](req.Params)
	if err != nil {
		writeJSONRPC(w, req.ID, &JSONRPCError{Code: ErrCodeInvalidParams, Message: err.Error()})
		return
	}

	task, err := h.tasks.CreateTask(r.Context(), params)
	if err != nil {
		writeJSONRPCFromError(w, req.ID, err)
		return
	}

	writeJSONRPC(w, req.ID, task)
}

func (h *JSONRPCHandler) handleTasksGet(w http.ResponseWriter, r *http.Request, req JSONRPCRequest) {
	params, err := unmarshalParams[TaskQueryParams](req.Params)
	if err != nil {
		writeJSONRPC(w, req.ID, &JSONRPCError{Code: ErrCodeInvalidParams, Message: err.Error()})
		return
	}

	task, err := h.tasks.GetTask(r.Context(), params.ID)
	if err != nil {
		writeJSONRPCFromError(w, req.ID, err)
		return
	}

	writeJSONRPC(w, req.ID, task)
}

func (h *JSONRPCHandler) handleTasksCancel(w http.ResponseWriter, r *http.Request, req JSONRPCRequest) {
	params, err := unmarshalParams[TaskQueryParams](req.Params)
	if err != nil {
		writeJSONRPC(w, req.ID, &JSONRPCError{Code: ErrCodeInvalidParams, Message: err.Error()})
		return
	}

	task, err := h.tasks.CancelTask(r.Context(), params.ID)
	if err != nil {
		writeJSONRPCFromError(w, req.ID, err)
		return
	}

	writeJSONRPC(w, req.ID, task)
}

func unmarshalParams[T any](raw any) (T, error) {
	var result T
	data, err := json.Marshal(raw)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	return result, nil
}

// writeJSONRPC writes a JSON-RPC 2.0 response. If resultOrError is a *JSONRPCError,
// it's placed in the error field; otherwise it's the result.
func writeJSONRPC(w http.ResponseWriter, id any, resultOrError any) {
	resp := JSONRPCResponse{JSONRPC: "2.0", ID: id}
	if e, ok := resultOrError.(*JSONRPCError); ok {
		resp.Error = e
	} else {
		resp.Result = resultOrError
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeJSONRPCFromError(w http.ResponseWriter, id any, err error) {
	var a2aErr *A2AError
	if errors.As(err, &a2aErr) {
		writeJSONRPC(w, id, &JSONRPCError{Code: a2aErr.Code, Message: a2aErr.Message})
		return
	}
	writeJSONRPC(w, id, &JSONRPCError{Code: -32603, Message: err.Error()})
}
