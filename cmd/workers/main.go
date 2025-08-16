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
	"sync"
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
	RetryPayloadHash     string
}

type paymentResp struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestDate   string  `json:"requestedAt,omitempty"`
}

type paymentRespCounter struct {
	Counter int `json:"counterDLQ"`
	paymentResp
}

var paymentPool = sync.Pool{
	New: func() any {
		return &paymentRespCounter{}
	},
}

var ctx context.Context = context.Background()

func (wrk *Worker) WorkerLaunchIN() {
	paymentBuf := paymentPool.Get().(*paymentRespCounter)
	defer func() {
		*paymentBuf = paymentRespCounter{}
		paymentPool.Put(paymentBuf)
	}()
	var jobJSON string
	var correlationID string
	var temp paymentResp
	for {
		result, err := wrk.redisClient.Client.BLPop(ctx, 1*time.Second, wrk.queueNameIN).Result()
		if err == redis.Nil {
			continue
		}
		if err == nil && len(result) > 1 {
			jobJSON = result[1]
			// Extraindo CorrelationID do JSON
			if err := json.Unmarshal([]byte(jobJSON), &temp); err != nil {
				log.Printf("JSON inválido na fila IN: %v", err)
				continue
			}
			correlationID = temp.CorrelationID
		}
		// Faz Unmarshal do JSON
		if err := json.Unmarshal([]byte(jobJSON), &paymentBuf); err != nil {
			log.Printf("Erro ao processar JSON: %v", err)
			continue
		}

		paymentWithCounter := paymentRespCounter{
			Counter:     paymentBuf.Counter,
			paymentResp: paymentBuf.paymentResp,
		}

		// Processa o pagamento
		wrk.processPayment(paymentWithCounter, correlationID)
	}
}
func (wrk *Worker) processPayment(job paymentRespCounter, correlationID string) {
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
				wrk.redisClient.Client.ZAdd(ctx, wrk.queueNameOutDefault, redis.Z{
					Score:  float64(score_unix),
					Member: jobJson,
				})
				fmt.Printf("Colocado no HSET %v\n ", wrk.queueNameOutDefault)
				fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)

			} else {
				wrk.redisClient.Client.ZAdd(ctx, wrk.queueNameOutFallback, redis.Z{
					Score:  float64(score_unix),
					Member: jobJson,
				})
				fmt.Printf("Colocado no HSET %v\n ", wrk.queueNameOutFallback)
				fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
			}

		} else {
			if job.Counter >= 4 {
				wrk.redisClient.Client.ZAdd(ctx, constants.DLQ, redis.Z{
					Score:  float64(score_unix),
					Member: jobJson,
				})
				wrk.redisClient.Client.HDel(ctx, wrk.RetryPayloadHash, correlationID)
			} else {
				job.Counter += 1
				jobJson, _ := json.Marshal(job)
				wrk.redisClient.Client.ZAdd(ctx, wrk.queueNameRetry, redis.Z{
					Score:  float64(score_unix),
					Member: jobJson,
				})

				wrk.redisClient.Client.HSet(ctx, wrk.RetryPayloadHash, correlationID, jobJson)
				wrk.redisClient.Client.ZAddNX(ctx, wrk.queueNameRetry, redis.Z{
					Score:  float64(score_unix),
					Member: correlationID,
				})

				fmt.Printf("Colocado no HSET %v - counter : %v\n ", wrk.queueNameRetry, job.Counter)
				fmt.Printf("Job processado. ID: %v - CorrId: %v | ReqAt: %v | Amount: %v", wrk.Id, job.CorrelationID, job.RequestDate, job.Amount)
			}
		}

	}

}

func (wrk *Worker) WorkerLaunchRetry() {
	paymentBuf := paymentPool.Get().(*paymentRespCounter)
	defer func() {
		*paymentBuf = paymentRespCounter{}
		paymentPool.Put(paymentBuf)
	}()

	var correlationID string

	for {
		zres, err := wrk.redisClient.Client.ZPopMin(ctx, wrk.queueNameRetry, 1).Result()
		if err != nil && err != redis.Nil {
			log.Printf("Erro ao consumir ZSET: %v\n", err)
			continue
		}

		if len(zres) == 0 {
			continue
		}

		correlationID = zres[0].Member.(string)
		jobJSON, err := wrk.redisClient.Client.HGet(ctx, wrk.RetryPayloadHash, correlationID).Result()
		if err != nil {
			log.Printf("Erro ao ler payload do hash: %v\n", err)
			continue
		}
		if err := json.Unmarshal([]byte(jobJSON), &paymentBuf); err != nil {
			log.Printf("Erro ao processar JSON: %v\n", err)
			continue
		}

		paymentWithCounter := paymentRespCounter{
			Counter:     paymentBuf.Counter,
			paymentResp: paymentBuf.paymentResp,
		}
		wrk.processPayment(paymentWithCounter, correlationID)
	}
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
		workerObjIN := &Worker{
			Id:                   i,
			redisClient:          repository.RedisClientSingleton,
			queueNameIN:          constants.QueueNameIN,
			queueNameRetry:       constants.QueueNameRetry,
			queueNameOutDefault:  constants.QueueNameOutDefault,
			queueNameOutFallback: constants.QueueNameOutFallback,
			CacheKey:             constants.CacheKey,
			PaymentDefaultURL:    constants.PaymentDefaultURL,
			PaymentFallbackURL:   constants.PaymentFallbackURL,
			RetryPayloadHash:     constants.RetryPayloadHash,
		}
		fmt.Printf("Lançado WorkerIN %v\n", workerObjIN.Id)
		go workerObjIN.WorkerLaunchIN()

	}

	for i := 0; i < 15; i++ {
		workerObjRetry := &Worker{
			Id:                   i,
			redisClient:          repository.RedisClientSingleton,
			queueNameIN:          constants.QueueNameIN,
			queueNameRetry:       constants.QueueNameRetry,
			queueNameOutDefault:  constants.QueueNameOutDefault,
			queueNameOutFallback: constants.QueueNameOutFallback,
			CacheKey:             constants.CacheKey,
			PaymentDefaultURL:    constants.PaymentDefaultURL,
			PaymentFallbackURL:   constants.PaymentFallbackURL,
			RetryPayloadHash:     constants.RetryPayloadHash,
		}
		fmt.Printf("Lançado WorkerRetry %v\n", workerObjRetry.Id)
		go workerObjRetry.WorkerLaunchRetry()
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
