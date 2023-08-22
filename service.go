package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

type OrderData struct {
	OrderUID    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
	Entry       string `json:"entry"`
	Delivery    struct {
		Name    string `json:"name"`
		Phone   string `json:"phone"`
		Zip     string `json:"zip"`
		City    string `json:"city"`
		Address string `json:"address"`
		Region  string `json:"region"`
		Email   string `json:"email"`
	} `json: "delivery"`
	Payment struct {
		Transaction  string  `json:"transaction"`
		RequestID    string  `json:"request_id"`
		Currency     string  `json:"currency"`
		Provider     string  `json:"provider"`
		Amount       float64 `json:"amount"`
		PaymentDT    float64 `json:"payment_dt"`
		Bank         string  `json:"bank"`
		DeliveryCost float64 `json:"delivery_cost"`
		GoodsTotal   float64 `json:"goods_total"`
		CustomFee    float64 `json:"custom_fee"`
	} `json: "payment"`
	Items []struct {
		ChrtID      float64 `json:"chrt_id"`
		TrackNumber string  `json:"track_number"`
		Price       float64 `json:"price"`
		RID         string  `json:"rid"`
		Name        string  `json:"name"`
		Sale        float64 `json:"sale"`
		Size        string  `json:"size"`
		TotalPrice  float64 `json:"total_price"`
		NMID        float64 `json:"nm_id"`
		Brand       string  `json:"brand"`
		Status      float64 `json:"status"`
	} `json: "items"`
	Locale            string  `json:"locale"`
	InternalSignature string  `json:"internal_signature"`
	CustomerID        string  `json:"customer_id"`
	DeliveryService   string  `json:"delivery_service"`
	Shardkey          string  `json:"shardkey"`
	SMID              float64 `json:"sm_id"`
	DateCreated       string  `json:"date_created"`
	OofShard          string  `json:"oof_shard"`
}

var cah = make(map[string]OrderData, 20) //Создание Кэша

func Cache(value []byte) { // Метод, сохраняющий полученные данные в Кэш
	var order OrderData
	err := json.Unmarshal(value, &order)
	if err != nil {
		log.Fatal(err)
	}
	cah[order.OrderUID] = order
	fmt.Println(cah)
}

func AddBase(file []byte) { // Метод добавления данных в базу
	connect := "user=postgres password=itpro25 dbname=WB sslmode=disable"
	db, err := sql.Open("postgres", connect)
	if err != nil {
		log.Fatalf("Ошибка открытия доступа к Субд: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(fmt.Sprintf("insert into \"fileJson\" (model_file) values ('%s')", string(file)))
	if err != nil {
		log.Fatalf("Ошибка добавления данных в таблицу: %v", err)
	}
}

func getDataByID(c *gin.Context) { //  Интерфейc, для извлечения данных по id из Кэша, в браузере
	idStr := c.Param("id")
	result, ok := cah[idStr]
	if ok {
		resultJSON, err := json.MarshalIndent(result, "", "\t")
		if err != nil {
			panic(err)
		}
		c.HTML(http.StatusOK, "data.html", gin.H{
			"data": string(resultJSON),
		})
		return
	}
	c.IndentedJSON(http.StatusNotFound, gin.H{"error": "Данные не найдены"})
}

func main() {
	var order OrderData // переменная типа структуры OrderData, куда помещается полученный файл Json

	subject := "my-chanel"
	clientID := "d6b085a4-1222-4a4b-9a31-550ab7dd8760"
	sc, err := stan.Connect("test-cluster", clientID, stan.NatsURL("nats://localhost:5111")) // Подключение к серверу nats-streaming
	if err != nil {
		log.Fatalf("Ошибка подключения к nats-sreaming server: %v", err)
	}
	defer sc.Close()

	data, err := ioutil.ReadFile("model2.json") // Чтение файла model.json перед пушем на сервер
	if err != nil {
		log.Fatalf("Ошибка конв-ции файла json в байтовый срез: %v", err)
	}

	err = json.Unmarshal(data, &order) // Проерка на валидность входящий данных (Приведение json к структуре OrderData)
	if err != nil {
		log.Fatalf("Не валидные данные. Ошибка помещения сообщения в структуру OrderData: %v", err)
		return
	}

	sub, err := sc.Subscribe(subject, func(msg *stan.Msg) {
		AddBase(msg.Data)
		Cache(msg.Data)
	}) // Подписка на канал
	if err != nil {
		log.Fatalf("Ошибка подписки на канал %s: %v\n", subject, err)
	}
	defer sub.Unsubscribe()
	time.Sleep(time.Second * 10)

	err = sc.Publish(subject, data) // Отправка данных на сервер
	if err != nil {
		log.Fatalf("Ошибка публикации сообщения: %v", err)
	}

	server := gin.Default()
	server.Static("/assets", "./assets")
	server.LoadHTMLGlob("templates/*.html")
	server.GET("/data/:id", getDataByID)
	server.Run(":8000")

}
