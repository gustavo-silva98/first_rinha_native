package healthcheck

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type ServiceHealthJSON struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type HealthChecker struct {
	//redisClient        *redis.Client
	CacheKey           string
	PaymentDefaultURL  string
	PaymentFallbackURL string
	UpdateFreq         time.Duration
	TTL                time.Duration
}

func (checker *HealthChecker) CheckHealth() (string, error) {

	jsonStatusDefault, err := checker.getHealthRequest(checker.PaymentDefaultURL + "/payments/service-health")
	if err != nil {
		fmt.Printf("Erro ao extrair Status: %v\n", err)
		return "", err
	}
	jsonStatusFallback, err := checker.getHealthRequest(checker.PaymentFallbackURL + "/payments/service-health")
	if err != nil {
		fmt.Printf("Erro ao extrair status: %v\n", err)
		return "", err
	}

	fmt.Println(jsonStatusDefault)
	fmt.Println(jsonStatusFallback)
	switch {
	case jsonStatusDefault.Failing && !jsonStatusFallback.Failing:
		fmt.Println("Ativou case 1")
		return checker.PaymentFallbackURL, nil

	case jsonStatusFallback.Failing && !jsonStatusDefault.Failing:
		fmt.Println("Ativou case 2")
		return checker.PaymentFallbackURL, nil

	case jsonStatusDefault.MinResponseTime <= jsonStatusFallback.MinResponseTime:
		fmt.Println("Ativou case 3")
		return checker.PaymentDefaultURL, nil

	case jsonStatusFallback.MinResponseTime < jsonStatusDefault.MinResponseTime:
		fmt.Println("Ativou case 4")
		return checker.PaymentFallbackURL, nil

	default:
		fmt.Println("Ativou case 5")
		return "", nil
	}

}

func (checker *HealthChecker) getHealthRequest(url string) (ServiceHealthJSON, error) {
	var jsonStatusHealth ServiceHealthJSON

	resp, err := http.Get(url)
	if err != nil {
		return jsonStatusHealth, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Erro ao ler body  read all: %v\n", err)
		return jsonStatusHealth, err
	}

	if err := json.Unmarshal(body, &jsonStatusHealth); err != nil {
		log.Printf("Erro ao ler body do unmarshal : %v\n", err)
		return jsonStatusHealth, err
	}

	return jsonStatusHealth, nil
}

func (checker *HealthChecker) StartHealthChecker() {
	ticker := time.NewTicker(checker.UpdateFreq * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		urlOK, err := checker.CheckHealth()
		if err != nil {
			fmt.Printf("Erro ao Checar health %v\n", err)
		}

		fmt.Printf("Salvando no REDIS a url: %v\n", urlOK)
	}

}
