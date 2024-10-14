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
	"strconv"
	"time"
)

var ctx = context.Background()

func main() {
	log.Println("Starting Redis consumer...")

	redisURL := os.Getenv("REDIS_URL")
	sourceQueue := os.Getenv("REDIS_LIST")
	processingQueue := os.Getenv("PROCESSING_LIST")
	lambdaName := os.Getenv("LAMBDA_NAME")

	log.Printf("Connecting to Redis at %s", redisURL)
	rdb := initializeRedisClient(redisURL)
	defer rdb.Close()

	// Get batch size from env var or default to 10
	batchSizeStr := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil || batchSize <= 0 {
		batchSize = 10 // Default value
	}
	log.Printf("Batch size set to %d", batchSize)

	// Get batch TTL from env var (in seconds) or default to 5 seconds
	batchTTLStr := os.Getenv("BATCH_TTL")
	batchTTLSeconds, err := strconv.Atoi(batchTTLStr)
	if err != nil || batchTTLSeconds <= 0 {
		batchTTLSeconds = 5 // Default to 5 seconds
	}
	batchTTL := time.Duration(batchTTLSeconds) * time.Second
	log.Printf("Batch TTL set to %d seconds", batchTTLSeconds)

	// AWS Lambda setup
	log.Println("Setting up AWS Lambda client...")
	sess := session.Must(session.NewSession())
	lambdaClient := lambda.New(sess)

	for {
		// Step 1: Process any existing items in the processing queue
		processExistingItems(rdb, lambdaClient, processingQueue, lambdaName, batchSize)

		// Step 2: Move items from the source queue to the processing queue
		batch := make([]string, 0, batchSize)
		startTime := time.Now() // Start TTL timer for the batch

		for len(batch) < batchSize {
			item, err := rdb.Do(ctx, "BLMOVE", sourceQueue, processingQueue, "RIGHT", "LEFT", 0).Result()
			if err != nil {
				log.Printf("Error moving item: %v", err)
				break // Exit if no more items to move
			}
			batch = append(batch, fmt.Sprintf("%v", item))

			// Check if TTL has been reached, process batch even if it's smaller
			if time.Since(startTime) >= batchTTL {
				log.Printf("TTL reached after %d seconds, processing smaller batch", batchTTLSeconds)
				break
			}
		}

		// Process the batch even if it's smaller than batchSize or TTL reached
		if len(batch) > 0 {
			log.Printf("Processing batch with %d items...", len(batch))
			processBatch(lambdaClient, lambdaName, batch, rdb, processingQueue)
		} else {
			log.Println("No items in batch to process")
		}
	}
}

// processExistingItems using LMPOP for batch processing
func processExistingItems(rdb *redis.Client, lambdaClient *lambda.Lambda, processingQueue, lambdaName string, batchSize int) {
	for {
		// Use LMPOP to fetch up to batchSize items in one go
		result, err := rdb.Do(ctx, "LMPOP", 1, processingQueue, "LEFT", "COUNT", batchSize).Result()

		// Handle Redis nil case when there are no items in the list
		if err == redis.Nil {
			log.Println("No more items in the processing queue")
			return
		}

		// Handle actual errors
		if err != nil {
			log.Printf("Error fetching items using LMPOP: %v", err)
			return
		}

		// Parse the result (result will be in the form [queue_name, items])
		data, ok := result.([]interface{})
		if !ok || len(data) != 2 {
			log.Println("Unexpected response from LMPOP")
			return
		}

		items, ok := data[1].([]interface{})
		if !ok {
			log.Println("Failed to parse LMPOP result")
			return
		}

		// Convert the popped items to strings for processing
		batch := make([]string, len(items))
		for i, item := range items {
			batch[i] = fmt.Sprintf("%v", item)
		}

		// Process the batch
		log.Printf("Processing batch of %d items from processing queue...", len(batch))
		processBatch(lambdaClient, lambdaName, batch, rdb, processingQueue)
	}
}

// processBatch invokes AWS Lambda with a batch of items
func processBatch(lambdaClient *lambda.Lambda, functionName string, batch []string, rdb *redis.Client, processingQueue string) {
	log.Printf("Invoking Lambda function %s with %d items", functionName, len(batch))

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

	log.Printf("Lambda invoked successfully. StatusCode: %d", *result.StatusCode)

	// No need for LREM as items are already removed from Redis using LMPOP or BLMOVE
	log.Printf("Batch processed and removed from Redis")
}

// initializeRedisClient initializes the Redis client
func initializeRedisClient(redisURL string) *redis.Client {
	log.Printf("Initializing Redis client with URL: %s", redisURL)
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	opts.PoolSize = 50
	opts.MinIdleConns = 10
	opts.DialTimeout = 10 * time.Second
	opts.ReadTimeout = 10 * time.Second
	opts.WriteTimeout = 10 * time.Second

	rdb := redis.NewClient(opts)
	log.Println("Redis client initialized successfully")
	return rdb
}
