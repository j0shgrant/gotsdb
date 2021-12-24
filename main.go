package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/j0shgrant/gotsdb/internal/storage"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strings"
)

const httpPort = 8080

func main() {
	// initialise logging
	logger, err := zap.NewProduction()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	zap.ReplaceGlobals(logger)
	defer func() {
		_ = logger.Sync()
	}()

	// create storage service
	engine, err := storage.NewEngine("data", true)
	if err != nil {
		zap.S().Fatal(err)
	}

	// configure http routing
	router := mux.NewRouter()
	router.HandleFunc("/ready", func(_ http.ResponseWriter, _ *http.Request) {})
	router.HandleFunc("/collections", func(w http.ResponseWriter, _ *http.Request) {
		collections, err := engine.ListCollections()
		if err != nil {
			handleServerError(w, err)
			return
		}

		if err := json.NewEncoder(w).Encode(collections); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	router.HandleFunc("/collections/{collection}/{key}", func(w http.ResponseWriter, r *http.Request) {
		// extra path params
		vars := mux.Vars(r)
		id, exists := vars["collection"]
		if !exists {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		key, exists := vars["key"]
		if !exists {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// read value from collection for given key
		value, err := engine.ReadKey(id, key)
		if err != nil {
			handleServerError(w, err)
			return
		}

		// marshal response into ResponseWriter
		data := map[string]string{
			"data": value,
		}
		err = json.NewEncoder(w).Encode(data)
		if err != nil {
			handleServerError(w, err)
			return
		}
	})
	router.HandleFunc("/collections/{collection}/{key}/{value}", func(w http.ResponseWriter, r *http.Request) {
		// extra path params
		vars := mux.Vars(r)
		id, exists := vars["collection"]
		if !exists {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		key, exists := vars["key"]
		if !exists {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		value, exists := vars["value"]
		if !exists {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// read value from collection for given key
		err := engine.WriteKey(id, key, value)
		if err != nil {
			handleServerError(w, err)
			return
		}
	})

	// listen for traffic
	addr := fmt.Sprintf(":%d", httpPort)
	zap.S().Infof("Listening for traffic on [%s].", addr)
	err = http.ListenAndServe(addr, router)

	// handle shutdown
	if err != nil {
		// log fatal error from serving http traffic
		zap.S().Error(err)

		// flush collections to disk and surface any errors
		var errorMessages []string
		for _, err := range engine.FlushAllCollections() {
			errorMessages = append(errorMessages, err.Error())
		}

		zap.S().Fatalf("Encountered errors flushing collections to cold storage: [%s].", strings.Join(errorMessages, ","))
	}
}

func handleServerError(w http.ResponseWriter, err error) {
	// write header
	w.WriteHeader(http.StatusInternalServerError)

	// serialise response
	if err = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    http.StatusInternalServerError,
		"message": err.Error(),
	}); err != nil {
		zap.S().Error(err)
	}
}
