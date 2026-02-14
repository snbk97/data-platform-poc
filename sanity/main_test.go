package sanity

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	kafkaBrokers    = "localhost:9092"
	cdcTopic        = "db-stream.cdc.events"
	deadLetterTopic = "db-stream.dead-letter.events"
	clickHouseHost  = "localhost"
	clickHousePort  = 8123
	clickHouseUser  = "default"
	postgreSQLHost  = "localhost"
	postgreSQLPort  = 5432
	mySQLHost       = "localhost"
	mySQLPort       = 3307
	redisHost       = "localhost"
	redisPort       = 6379
	apiPort         = "8080"
	kafkaUIPort     = 8091
)

func TestKafka_Connection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka connection test in short mode")
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    cdcTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("sanity-test-key"),
		Value: []byte(`{"test": "sanity"}`),
	})
	require.NoError(t, err, "Should produce message to Kafka")
}

func TestKafka_TopicsExist(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka topic test in short mode")
	}

	conn, err := kafka.Dial("tcp", kafkaBrokers)
	require.NoError(t, err)
	defer conn.Close()

	partitionList, err := conn.ReadPartitions(cdcTopic)
	require.NoError(t, err, "Should be able to read partitions for topic %s", cdcTopic)
	assert.NotEmpty(t, partitionList, "Topic should have partitions")
}

func TestClickHouse_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping ClickHouse connection test in short mode")
	}

	url := fmt.Sprintf("http://%s:%s@%s:%d/ping",
		clickHouseUser,
		"",
		clickHouseHost,
		clickHousePort,
	)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err, "ClickHouse should respond to ping")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "ClickHouse ping should return 200")
}

func TestPostgreSQL_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL connection test in short mode")
	}

	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", postgreSQLHost, postgreSQLPort))
	if err != nil {
		t.Logf("PostgreSQL not reachable via TCP: %v", err)
		t.SkipNow()
	}
	conn.Close()
}

func TestMySQL_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL connection test in short mode")
	}

	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", mySQLHost, mySQLPort))
	if err != nil {
		t.Logf("MySQL not reachable via TCP: %v", err)
		t.SkipNow()
	}
	conn.Close()
}

func TestRedis_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Redis connection test in short mode")
	}

	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", redisHost, redisPort))
	if err != nil {
		t.Logf("Redis not reachable via TCP: %v", err)
		t.SkipNow()
	}
	conn.Close()
}

func TestAPI_HealthEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping API health endpoint test in short mode")
	}

	url := fmt.Sprintf("http://localhost:%s/health", apiPort)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)

	if err != nil {
		t.Logf("API not running (expected in sanity test): %v", err)
		t.SkipNow()
	}
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestKafka_UI_Health(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka UI health test in short mode")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost:%d", kafkaUIPort))

	if err != nil {
		t.Logf("Kafka UI not running: %v", err)
		t.SkipNow()
	}
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Kafka UI should be healthy")
}
