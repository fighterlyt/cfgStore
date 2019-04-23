package model

import "fmt"

type CfgType string

const (
	JSON CfgType = "json"
	YAML         = "yaml"
	INVALID
)

var (
	InvalidError = fmt.Errorf("非法的类型")
)

func ConvertType(data string) (CfgType, error) {
	switch data {
	case string(JSON):
		return JSON, nil
	case string(YAML):
		return YAML, nil
	default:
		return INVALID, InvalidError
	}
}
