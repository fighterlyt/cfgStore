package web

import (
	"github.com/fighterlyt/cfgStore/model"
	"github.com/fighterlyt/cfgStore/store"
	"github.com/gin-gonic/gin"
	"log"
)

type Server struct {
	engine *gin.Engine
	store  store.Store
	port   string
}

func NewServer(port string, store store.Store) *Server {
	return &Server{
		engine: gin.Default(),
		port:   port,
		store:  store,
	}
}

func (s Server) Start() {
	s.engine.POST("/update", s.update)
	s.engine.GET("/:key/:version", s.get)
	if err := s.engine.Run(":" + s.port); err != nil {
		log.Fatal(err.Error())
	}
}

func (s Server) update(ctx *gin.Context) {
	req := &UpdateRequest{}
	if err := ctx.BindJSON(&req); err != nil {
		ctx.JSON(200, Response{
			ErrorCode: 1,
			Error:     err.Error(),
		})
	} else {
		if cfgTyp, err := model.ConvertType(req.Type); err != nil {
			ctx.JSON(200, Response{
				ErrorCode: 2,
				Error:     err.Error(),
				Data:      req.Data,
			})
		} else {
			if err = s.store.Write(req.Key, req.Data, cfgTyp); err != nil {
				ctx.JSON(500, Response{
					ErrorCode: 3,
					Error:     err.Error(),
					Data:      req.Data,
				})
			} else {
				ctx.JSON(200, Response{})
			}
		}

	}
}

func (s Server) get(ctx *gin.Context) {

}

type UpdateRequest struct {
	Key  string `json:"key"`
	Data string `json:"data"`
	Type string `json:"type"`
}

type Response struct {
	ErrorCode int         `json:"errCode"`
	Error     string      `json:"error"`
	Data      interface{} `json:"data"`
}
