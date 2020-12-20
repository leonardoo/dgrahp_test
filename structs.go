package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	//"regexp"
	"strconv"
	"strings"
)

type Buyer struct {
	Uid   string `json:"uid,omitempty"`
	Id   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Age  int    `json:"age,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func (buyer *Buyer) UnmarshalJSON(data []byte) error {
	buyer.DType = []string{"Buyer"} // set default value before unmarshaling
	type Alias Buyer // create alias to prevent endless loop
	tmp := (*Alias)(buyer)
	err := json.Unmarshal(data, tmp)
	if(err != nil){
		return err
	}
	tmp.Uid = fmt.Sprintf("_:b_%s", tmp.Id)
	fmt.Println(tmp.Uid, tmp.Id, tmp.Age, tmp.Name)
	return nil
}

type Product struct {
	Id    string `json:"uid,omitempty"`
	Name  string `json:"name,omitempty"`
	Price int    `json:"price,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

type Transaction struct {
	Uid         string   `json:"uid,omitempty"`
	Id         string   `json:"id,omitempty"`
	BuyerId    interface{}   `json:"buyer_id,omitempty"`
	Ip         string   `json:"ip,omitempty"`
	Device     string   `json:"device,omitempty"`
	ProductIds []Product `json:"product_ids,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func newTransaction(data string, buyers_ids map[string]string, products_ids map[string]string) (*Transaction, error) {
	data_splitted := strings.Split(data, ";")
	if len(data_splitted) != 5 {
		return nil, errors.New(fmt.Sprintf("invalid: data for transaction: %v", data))
	}
	buyer_id := buyers_ids[fmt.Sprintf("b_%s", data_splitted[1])]
	transaction := &Transaction{
		Id:      data_splitted[0],
		Uid:      fmt.Sprintf("_:t_%s", data_splitted[0]),
		BuyerId: Buyer{
			Uid: buyer_id,
		},
		Ip:      data_splitted[2],
		Device:  data_splitted[3],
		DType: []string{"Transaction"},
	}
	products := data_splitted[len(data_splitted)-1]
	if len(products) > 0 {
		re := regexp.MustCompile(`(\w+|,)+`)
		products_bytes := re.FindAll([]byte(products), -1)
		if len(products_bytes) > 0 {
			products := strings.Split(string(products_bytes[0]), ",")
			for _, value := range products {
				product_id := products_ids[fmt.Sprintf("p_%s", value)]
				transaction.ProductIds = append(transaction.ProductIds, Product{
					Id: product_id,
				})
			}
		}
	}
	return transaction, nil
}

func newProduct(data []string) (*Product, error) {
	if len(data) < 3 {
		return nil, errors.New(fmt.Sprintf("invalid: data for transaction: %v", data))
	}
	price, err := strconv.Atoi(data[2])
	if err != nil {
		return nil, err
	}
	return &Product{
		Id:    fmt.Sprintf("_:p_%s", data[0]),
		Name:  data[1],
		Price: price,
		DType: []string{"Product"},
	}, nil
}
