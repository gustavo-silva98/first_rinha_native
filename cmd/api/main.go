package main

import (
	"bytes"
	"context"
	"encoding/json"
	"firstApi/internal/constants"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Backend struct {
	redisClient *redis.Client
}

type paymentResp struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestDate   string  `json:"requestedAt,omitempty"`
}

type totalPayment struct {
	Requests int     `json:"totalRequests"`
	Amount   float64 `json:"totalAmount"`
}

type paymentSummary struct {
	Default  totalPayment `json:"default"`
	Fallback totalPayment `json:"fallback"`
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

func (api *Backend) paymentEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	dateRequest := time.Now().UTC().Format(time.RFC3339)

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
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

	paymentBuf.RequestDate = dateRequest

	paymentJson, err := json.Marshal(paymentBuf)
	if err != nil {
		http.Error(w, "Erro ao serializar o Json para colocar no Redis", http.StatusInternalServerError)
		return
	}

	err = api.redisClient.RPush(r.Context(), "payment-queue", paymentJson).Err()
	if err != nil {
		http.Error(w, "Erro ao enfileirar payment", http.StatusInternalServerError)
		return
	} else {
		fmt.Println("Payment enfileirado.")
	}

}

func (api *Backend) paymentSummaryEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
	urlParse, err := url.Parse(r.URL.String())
	if err != nil {
		fmt.Println("Erro ao Parsear url ", err)
	}
	from := urlParse.Query().Get("from")
	to := urlParse.Query().Get("to")
	fromTimeParsed, _ := time.Parse(time.RFC3339Nano, from)
	toTimeParsed, _ := time.Parse(time.RFC3339Nano, to)
	fromTimeUnix := fromTimeParsed.Unix()
	toTimeUnix := toTimeParsed.Unix()
	fmt.Println("O FROM é ", from, " UNIX= ", fromTimeUnix)
	fmt.Println("O To É ", to, "UNIX= ", toTimeUnix)

	resultDefault := api.readSingleSortedSetTESTE(constants.QueueNameOutDefault, int(fromTimeUnix), int(toTimeUnix))
	resultFallback := api.readSingleSortedSetTESTE(constants.QueueNameOutFallback, int(fromTimeUnix), int(toTimeUnix))

	summaryFinal := paymentSummary{
		Default:  resultDefault,
		Fallback: resultFallback,
	}

	if err := json.NewEncoder(w).Encode(summaryFinal); err != nil {
		http.Error(w, "Erro ao gerar json final", http.StatusInternalServerError)
	}
}

func (api *Backend) readRedisList(w http.ResponseWriter, r *http.Request) {
	items, err := api.redisClient.LRange(ctx, "payment-queue", 0, -1).Result()
	if err != nil {
		http.Error(w, "Erro ao ler itens da fila", http.StatusInternalServerError)
		return
	}

	paymentsRedis := make([]paymentResp, 1000)
	var countValue float64
	var p paymentResp
	for _, itemStr := range items {

		if err := json.Unmarshal([]byte(itemStr), &p); err != nil {
			log.Printf("Erro ao fazer unmarshal : %v ", err)
			continue
		}
		countValue += p.Amount
		paymentsRedis = append(paymentsRedis, p)
	}

	log.Printf("Soma das transações é: %v ", countValue)
	if err := json.NewEncoder(w).Encode(paymentsRedis); err != nil {
		http.Error(w, "Erro ao gerar json final", http.StatusInternalServerError)
	}
}

func (api *Backend) readSingleSortedSet(SetName string) totalPayment {
	result, err := api.redisClient.ZRevRangeByScore(ctx, SetName, &redis.ZRangeBy{
		Min: "-Inf",
		Max: "+Inf",
	}).Result()

	if err != nil {
		fmt.Printf("Falha ao ler resultado da Sorted Set %v\n", err)
		return totalPayment{}
	}

	paymentBuf := paymentPool.Get().(*paymentResp)
	var countValue float64

	defer func() {
		*paymentBuf = paymentResp{}
		paymentPool.Put(paymentBuf)
	}()

	for _, resultStr := range result {
		if err := json.Unmarshal([]byte(resultStr), &paymentBuf); err != nil {
			fmt.Printf("Erro ao converter %v\n", err)
			continue
		} else {
			countValue += paymentBuf.Amount
		}
	}
	paymentStruct := totalPayment{
		Requests: len(result),
		Amount:   countValue,
	}

	return paymentStruct
}

func (api *Backend) readSingleSortedSetTESTE(SetName string, min int, max int) totalPayment {
	minStr := strconv.Itoa(min)
	maxStr := strconv.Itoa(max)

	result, err := api.redisClient.ZRevRangeByScore(ctx, SetName, &redis.ZRangeBy{
		Min: minStr,
		Max: maxStr,
	}).Result()

	if err != nil {
		fmt.Printf("Falha ao ler resultado da Sorted Set %v\n", err)
		return totalPayment{}
	}

	paymentBuf := paymentPool.Get().(*paymentResp)
	var countValue float64

	defer func() {
		*paymentBuf = paymentResp{}
		paymentPool.Put(paymentBuf)
	}()

	for _, resultStr := range result {
		if err := json.Unmarshal([]byte(resultStr), &paymentBuf); err != nil {
			fmt.Printf("Erro ao converter %v\n", err)
			continue
		} else {
			countValue += paymentBuf.Amount
		}
	}
	paymentStruct := totalPayment{
		Requests: len(result),
		Amount:   countValue,
	}

	return paymentStruct
}

func (api *Backend) readResultRedis(w http.ResponseWriter, r *http.Request) {
	resultDefault := api.readSingleSortedSet(constants.QueueNameOutDefault)
	resultFallback := api.readSingleSortedSet(constants.QueueNameOutFallback)

	summaryFinal := paymentSummary{
		Default:  resultDefault,
		Fallback: resultFallback,
	}

	if err := json.NewEncoder(w).Encode(summaryFinal); err != nil {
		http.Error(w, "Erro ao gerar json final", http.StatusInternalServerError)
	}
}

func (api *Backend) readDLQ(w http.ResponseWriter, r *http.Request) {
	result, err := api.redisClient.ZRevRangeByScore(ctx, constants.DLQ, &redis.ZRangeBy{
		Min: "-Inf",
		Max: "+Inf",
	}).Result()

	if err != nil {
		fmt.Printf("Falha ao ler resultado da Sorted Set %v\n", err)
	}
	json.NewEncoder(w).Encode(result)

}

func pingRedis(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		pong, err := rdb.Ping(ctx).Result()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(pong)
	}
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

	client := redis.NewClient(&redis.Options{
		Addr:     "redis-rinha:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	router := http.NewServeMux()

	registerPprof(router)

	api := &Backend{
		redisClient: client,
	}

	err := api.redisClient.Del(ctx, "payment-queue").Err()
	if err != nil {
		panic(err)
	}

	router.HandleFunc("/read-redis", api.readRedisList)
	router.HandleFunc("/payments", api.paymentEndpoint)
	router.HandleFunc("/payments-summary", api.paymentSummaryEndpoint)
	router.HandleFunc("/ping-redis", pingRedis(client))
	router.HandleFunc("/result", api.readResultRedis)
	router.HandleFunc("/dlq", api.readDLQ)

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
