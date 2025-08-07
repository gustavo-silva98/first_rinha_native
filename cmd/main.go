package main

import (
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
)

type message struct {
	Message string `json:"message"`
	ID      int    `json:"id"`
}

var messages = map[int]message{
	1: {Message: "Hello World", ID: 1},
	2: {Message: "Bye World", ID: 2},
}

func getMessages(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, messages)
}

func getMessageID(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}
	lookup, ok := messages[id]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Message not found"})
		return
	}

	c.IndentedJSON(http.StatusOK, lookup)
}

func returnID(id string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"id": id,
		})
	}
}

func main() {
	apiId := os.Getenv("API_ID")
	port := os.Getenv("PORT")
	router := gin.Default()
	router.GET("/messages", getMessages)
	router.GET("/messages/:id", getMessageID)
	router.GET("/api", returnID(apiId))

	router.Run(":" + port)
}
