package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Id                   int
	redisClient          *redis.Client
	queueNameIN          string
	queueNameRetry       string
	queueNameOutDefault  string
	queueNameOutFallback string
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
		result, err := wrk.redisClient.BLPop(ctx, 1*time.Second, wrk.queueNameIN, wrk.queueNameRetry).Result()
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

		// Tooooda a logica de consultar status e fazer a request pra pagamento aqui

		wrk.redisClient.ZAdd(ctx, wrk.queueNameOutDefault, redis.Z{Score: float64(score_unix), Member: jobJson})
	}

	fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
}

func main() {
	fmt.Println("Subindo o Worker ")

	client := redis.NewClient(&redis.Options{
		Addr:     "redis-rinha:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	for i := 0; i < 15; i++ {
		workerObj := &Worker{
			Id:                   i,
			redisClient:          client,
			queueNameIN:          "payment-queue",
			queueNameRetry:       "payment-retry-queue",
			queueNameOutDefault:  "payment-result-default",
			queueNameOutFallback: "payment-result-fallback",
		}
		fmt.Printf("Lançado Worker %v ", workerObj.Id)
		go workerObj.WorkerLaunch()

	}

	select {}
}
