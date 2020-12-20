package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
)

func doRequest(url string, datetime time.Time) ([]byte, error) {
	client := http.Client{
		Timeout: time.Second * 5, // Timeout after 2 seconds
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("date", strconv.FormatInt(datetime.Unix(), 10))
	req.URL.RawQuery = q.Encode()

	res, getErr := client.Do(req)
	if getErr != nil {
		return nil, getErr
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		return nil, readErr
	}
	return body, nil

}

func getBuyerData(datetime time.Time) ([]Buyer, error) {

	url := "https://kqxty15mpg.execute-api.us-east-1.amazonaws.com/buyers"
	body, err := doRequest(url, datetime)
	if err != nil {
		return nil, err
	}
	var buyers []Buyer
	jsonErr := json.Unmarshal(body, &buyers)
	if jsonErr != nil {
		return nil, jsonErr
	}
	return buyers, nil
}

func ReadCSVFromHttpRequest(url string, datetime time.Time) ([][]string, error) {
	client := http.Client{
		Timeout: time.Second * 2, // Timeout after 2 seconds
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}

	q := req.URL.Query()
	q.Add("date", strconv.FormatInt(datetime.Unix(), 10))
	req.URL.RawQuery = q.Encode()

	res, getErr := client.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	if res.Body != nil {
		defer res.Body.Close()
	}
	reader := csv.NewReader(res.Body)
	reader.Comma = '\''
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func getProductsData(datetime time.Time) ([]Product, error) {
	url := "https://kqxty15mpg.execute-api.us-east-1.amazonaws.com/products"
	body, err := ReadCSVFromHttpRequest(url, datetime)
	if err != nil {
		return nil, err
	}
	products := make([]Product, 0)
	for _, value := range body {
		product, err := newProduct(value)
		if err == nil {
			products = append(products, *product)
		}
	}
	return products, nil
}

func getTransactionData(datetime time.Time, buyers map[string]string, products map[string]string) ([]Transaction, error) {
	url := "https://kqxty15mpg.execute-api.us-east-1.amazonaws.com/transactions"
	body, err := doRequest(url, datetime)
	if err != nil {
		return nil, err
	}
	res1 := bytes.Split(body, []byte("\x00\x00"))
	transactions := make([]Transaction, 0)
	for _, value := range res1 {
		data := string(bytes.ReplaceAll(value, []byte("\x00"), []byte(";")))
		transaction, err := newTransaction(data, buyers, products)
		if err == nil {
			transactions = append(transactions, *transaction)
		}
	}

	return transactions, nil
}

func loadData(datetime time.Time) {
	ctx := context.TODO()
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	txn := dgraphClient.NewTxn()
	defer txn.Commit(ctx)
	op := &api.Operation{
		Schema: `
		  id: string @index(hash) .
		  age: int .
		  price: int .
		  name: string .
          buyer_id: uid @reverse .
		  ip: string @index(hash) .
		  device: string .
	      product_ids: [uid] @reverse .

		  type Buyer {
			name
			age
		  }
		
		  type Product {
			name
			price
		  }

  		  type Transaction {
			buyer_id
			ip
			device
			product_ids
          }
		`,
	}
	err = dgraphClient.Alter(ctx, op)
	if err != nil {
		log.Fatal("failed to marshal ", err)
	}
	buyers, err := getBuyerData(datetime)
	if err != nil {
		log.Fatal(err)
	}
	buyersJson, err := json.Marshal(buyers)
	if err != nil {
		log.Fatal("failed to marshal buyers", err)
	}
	mu := &api.Mutation{
		SetJson: buyersJson,
	}
	res, err := txn.Mutate(ctx, mu)
	if err != nil {
		log.Fatal("failed to mutate ", err)
	}
	buyers_uids := res.Uids

	products, err := getProductsData(datetime)
	if err != nil {
		log.Fatal(err)
	}

	productsJson, err := json.Marshal(products)
	if err != nil {
		log.Fatal("failed to marshal products", err)
	}

	mu = &api.Mutation{
		SetJson: productsJson,
	}

	res, err = txn.Mutate(ctx, mu)
	if err != nil {
		log.Fatal("failed to mutate ", err)
	}
	product_uids := res.Uids

	transactions, err := getTransactionData(datetime, buyers_uids, product_uids)
	if err != nil {
		log.Fatal(err)
	}
	transactionsJson, err := json.Marshal(transactions)
	if err != nil {
		log.Fatal("failed to marshal transactions", err)
	}

	mu = &api.Mutation{
		SetJson: transactionsJson,
	}


	res, err = txn.Mutate(ctx, mu)
	if err != nil {
		log.Fatal("failed to mutate ", err)
	}
	print("res: %v", res)
}
