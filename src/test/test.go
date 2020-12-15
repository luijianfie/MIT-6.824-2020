package main

import "fmt"


func main(){

	m1 :=make(map[string]string)
	m2 :=make(map[string]string)
	m3 :=make(map[string]string)

	m1["age"]="1"
	m1["age1"]="23"
	m2["age"]="2"

	append(m3,m2)
	append(m3,m1)

	fmt.Println(m3)
}