package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
)

var ctx = context.Background()

const batchSize = 10 // Number of items to process in a batch

func main() {
	redisURL := os.Getenv("REDIS_URL")
	sourceQueue := os.Getenv("REDIS_LIST")
	processingQueue := os.Getenv("PROCESSING_LIST")
	lambdaName := os.Getenv("LAMBDA_NAME")

	rdb := initializeRedisClient(redisURL)
	defer rdb.Close()

	// AWS Lambda setup
	sess := session.Must(session.NewSession())
	lambdaClient := lambda.New(sess)

	for {
		// Step 1: Process any existing items in the processing queue
		processExistingItems(rdb, lambdaClient, processingQueue, lambdaName)

		// Step 2: Move items from the source queue to the processing queue
		batch := make([]string, 0, batchSize)
		for len(batch) < batchSize {
			item, err := rdb.Do(ctx, "BLMOVE", sourceQueue, processingQueue, "RIGHT", "LEFT", 0).Result()
			if err != nil {
				log.Printf("Error moving item: %v", err)
				break // Exit if no more items to move
			}
			batch = append(batch, fmt.Sprintf("%v", item))
		}

		// Process the batch even if it's less than batchSize
		if len(batch) > 0 {
			processBatch(lambdaClient, lambdaName, batch, rdb, processingQueue)
		}
	}
}

// processExistingItems processes any items that are already in the processing queue
func processExistingItems(rdb *redis.Client, lambdaClient *lambda.Lambda, processingQueue, lambdaName string) {
	for {
		batch := make([]string, 0, batchSize)

		// Try to fetch up to batchSize items from the processing queue
		for len(batch) < batchSize {
			item, err := rdb.LPop(ctx, processingQueue).Result() // Pop from processing queue
			if err == redis.Nil {
				return // No more items in the processing queue
			} else if err != nil {
				log.Printf("Error popping item from processing queue: %v", err)
				return
			}
			batch = append(batch, fmt.Sprintf("%v", item))
		}

		// Process the batch
		processBatch(lambdaClient, lambdaName, batch, rdb, processingQueue)
	}
}

// processBatch invokes AWS Lambda with a batch of items and removes them from the processing queue
func processBatch(lambdaClient *lambda.Lambda, functionName string, batch []string, rdb *redis.Client, processingQueue string) {
	payload, err := json.Marshal(batch)
	if err != nil {
		log.Printf("Error marshaling batch to JSON: %v", err)
		return
	}

	input := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}

	result, err := lambdaClient.Invoke(input)
	if err != nil {
		log.Printf("Error invoking lambda: %v", err)
		return
	}

	log.Printf("Lambda invoked successfully. StatusCode: %d, Payload: %s", *result.StatusCode, string(result.Payload))

	// Acknowledge items by removing them from the processing queue
	for _, item := range batch {
		_, err := rdb.LRem(ctx, processingQueue, 1, item).Result()
		if err != nil {
			log.Printf("Error removing item from processing queue: %v", err)
		}
	}
}

// initializeRedisClient initializes the Redis client
func initializeRedisClient(redisURL string) *redis.Client {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	rdb := redis.NewClient(opts)
	return rdb
}
