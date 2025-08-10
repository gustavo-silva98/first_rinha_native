package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"
)

type paymentResp struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

var paymentPool = sync.Pool{
	New: func() any {
		return &paymentResp{}
	},
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func paymentEndpoint(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()

	paymentBuf := paymentPool.Get().(*paymentResp)
	reqBuf := bufferPool.Get().(*bytes.Buffer)

	defer func() {
		reqBuf.Reset()
		bufferPool.Put(reqBuf)
		*paymentBuf = paymentResp{}
		paymentPool.Put(paymentBuf)
	}()

	if _, err := io.Copy(reqBuf, r.Body); err != nil {
		http.Error(w, "Erro ao ler body", http.StatusBadRequest)
		return
	}

	if err := json.Unmarshal(reqBuf.Bytes(), paymentBuf); err != nil {
		http.Error(w, "JSON Inválido", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(reqBuf.Bytes())

}

func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func main() {
	if runtime.NumCPU()*4 > 32 {
		runtime.GOMAXPROCS(32)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU() * 4)
	}

	port := os.Getenv("PORT")

	router := http.NewServeMux()

	registerPprof(router)

	router.HandleFunc("/payments", paymentEndpoint)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	fmt.Println("Iniciando o Server")
	log.Fatal(server.ListenAndServe())
}
