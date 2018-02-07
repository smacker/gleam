package main

import (
	"github.com/chrislusf/gleam/gio"
	enry "gopkg.in/src-d/enry.v1"
)

func filterNotEmptyString(idx int) gio.Mapper {
	return func(x []interface{}) error {
		if x[idx].(string) == "" {
			return nil
		}
		return gio.Emit(x...)
	}
}

func filterIntMore(idx int, value int64) gio.Mapper {
	return func(x []interface{}) error {
		if x[idx].(int64) > value {
			return gio.Emit(x...)
		}
		return nil
	}
}

func classifyLanguage(filenameIdx, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[filenameIdx])
		content := gio.ToBytes(x[contentIdx])
		lang := enry.GetLanguage(filename, content)
		return gio.Emit(append(x, lang)...)
	}
}
