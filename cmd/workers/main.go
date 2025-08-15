package main

import (
	"bytes"
	"context"
	"encoding/json"
	"firstApi/internal/constants"
	"firstApi/internal/healthcheck"
	"firstApi/internal/repository"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Id                   int
	redisClient          *repository.RedisClient
	queueNameIN          string
	queueNameRetry       string
	queueNameOutDefault  string
	queueNameOutFallback string
	CacheKey             string
	PaymentDefaultURL    string
	PaymentFallbackURL   string
}

type paymentResp struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestDate   string  `json:"requestedAt,omitempty"`
}

var ctx context.Context = context.Background()

func (wrk *Worker) WorkerLaunch() {
	for {
		result, err := wrk.redisClient.Client.BLPop(ctx, 1*time.Second, wrk.queueNameIN, wrk.queueNameRetry).Result()
		if err == redis.Nil {
			continue
		}

		if err != nil {
			log.Printf("Erro ao consumir das filas: %v ", err)
			continue
		}
		fmt.Println("Job pego do Redis ", result)
		var paymentsRedis paymentResp
		if err := json.Unmarshal([]byte(result[1]), &paymentsRedis); err != nil {
			log.Printf("Erro ao processar json da fila. JSON Inválido: %v ", err)
			continue
		}

		wrk.processPayment(paymentsRedis)
	}

}

func (wrk *Worker) processPayment(job paymentResp) {
	score, err := time.Parse(time.RFC3339, job.RequestDate)
	if err != nil {
		log.Printf("Erro ao converter requestDate")
	} else {
		score_unix := score.Unix()
		jobJson, _ := json.Marshal(job)

		urlOk, err := wrk.redisClient.Client.Get(ctx, wrk.CacheKey).Result()
		if err != nil {
			fmt.Println("Erro ao ler instancia correta pra subir payment. ", err)
			return
		}
		fmt.Printf("Lido URL Redis: %v\n", urlOk)
		if postPayment := wrk.paymentPost(urlOk, jobJson); postPayment {
			if urlOk == wrk.PaymentDefaultURL {
				wrk.redisClient.Client.ZAdd(ctx, wrk.queueNameOutDefault, redis.Z{Score: float64(score_unix), Member: jobJson})
				fmt.Printf("Colocado no HSET %v\n ", wrk.queueNameOutDefault)
				fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
			} else {
				wrk.redisClient.Client.ZAdd(ctx, wrk.queueNameOutFallback, redis.Z{Score: float64(score_unix), Member: jobJson})
				fmt.Printf("Colocado no HSET %v\n ", wrk.queueNameOutFallback)
				fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
			}
		}

		// Tooooda a logica de consultar status e fazer a request pra pagamento aqui
	}

	fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
}

func (wrk *Worker) paymentPost(url string, job []byte) bool {
	payload := bytes.NewBuffer(job)

	resp, err := http.Post(url+"/payments", "application/json", payload)
	if err != nil {
		log.Printf("Falha ao fazer post no endpoint: %v\n", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("Falha no Post Payment: %v\n", resp.StatusCode)
		return false
	}
	return true
}

func main() {
	fmt.Println("Subindo o Worker ")

	for i := 0; i < 15; i++ {
		workerObj := &Worker{
			Id:                   i,
			redisClient:          repository.RedisClientSingleton,
			queueNameIN:          constants.QueueNameIN,
			queueNameRetry:       constants.QueueNameRetry,
			queueNameOutDefault:  constants.QueueNameOutDefault,
			queueNameOutFallback: constants.QueueNameOutFallback,
			CacheKey:             constants.CacheKey,
			PaymentDefaultURL:    constants.PaymentDefaultURL,
			PaymentFallbackURL:   constants.PaymentFallbackURL,
		}
		fmt.Printf("Lançado Worker %v ", workerObj.Id)
		go workerObj.WorkerLaunch()

	}

	healthcheck := &healthcheck.HealthChecker{
		CacheKey:           constants.CacheKey,
		PaymentDefaultURL:  constants.PaymentDefaultURL,
		PaymentFallbackURL: constants.PaymentFallbackURL,
		UpdateFreq:         constants.UpdateFreq * time.Second,
		TTL:                constants.TTL * time.Second,
		RedisClient:        repository.RedisClientSingleton,
	}
	go healthcheck.StartHealthChecker()

	select {}
}
