# Stage 1: Build the Go application using the official Golang image
FROM golang:1.18 as builder

# Metadata about the image
LABEL authors="gabriel.cerioni"

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files to the container for dependency management
COPY go.mod go.sum ./

# Download the Go module dependencies
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the Go binary
RUN go build -o app .

# Stage 2: Create the final runtime container using a smaller image
FROM gcr.io/distroless/base-debian11

# Set the working directory inside the container
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/app .

# Define environment variables that can be tuned by the customer
# These can be overridden when running the container
ENV REDIS_URL=redis://localhost:6379
ENV REDIS_LIST=source_list
ENV PROCESSING_LIST=batch_processing
ENV LAMBDA_NAME=my-lambda-function
ENV BATCH_SIZE=10
ENV BATCH_TTL=5
ENV AWS_REGION=us-east-1

# Environment variables for AWS credentials (optional)
# These can be overridden or handled via IAM roles or EC2 instance profiles
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_SESSION_TOKEN=""

# Command to run the Go binary
CMD ["./app"]