package env

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func GetEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func GetEnvIntOrDefault(key string, defaultValue int) int {
	value := GetEnvOrDefault(key, strconv.Itoa(defaultValue))
	parsedValue, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Invalid value for %s: %s. Using default %d", key, value, defaultValue)
		return defaultValue
	}
	return parsedValue
}

func WarnIfEnvFileNotFound() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, using default values")
	}
}
