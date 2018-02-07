package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
)

// PROGRAMMING LANGUAGES
// 1. What are the ten most used programming languages?
// 2. How many projects use more than one programming language?
// 3. How many projects use the Scheme programming language? (Gradle instead of Scheme)

var classifyLanguageMapper = gio.RegisterMapper(classifyLanguage(4, 3))
var filterEmptyLangMapper = gio.RegisterMapper(filterNotEmptyString(7))
var groupCountMapper = gio.RegisterMapper(func(x []interface{}) error {
	key := x[0]
	count := len(x) - 1
	return gio.Emit(key, count)
})

func mostUsedLanguages(path string) *flow.Dataset {
	numberOfLangs := 10

	fmt.Printf(">>> %d most used languages:\n", numberOfLangs)
	return getBlobs("mostUsedLanguages", path).
		Map("classifyLanguage", classifyLanguageMapper).
		Map("filter empty lang", filterEmptyLangMapper).
		GroupBy("group by lang", flow.Field(8)).
		Map("group count", groupCountMapper).
		Sort("sortBySum", flow.OrderBy(2, true)).
		Top("top", numberOfLangs, flow.OrderBy(2, false)).
		Printlnf("%s %d")
}

var countLangsMapper = gio.RegisterMapper(func(x []interface{}) error {
	key := x[0]
	langs := make(map[string]bool)
	for _, item := range x[1:] {
		lang := item.([]interface{})[6].(string)
		langs[lang] = true
	}
	return gio.Emit(key, len(langs))
})
var filterMoreOneLang = gio.RegisterMapper(filterIntMore(1, 1))
var toOneMapper = gio.RegisterMapper(func(x []interface{}) error {
	return gio.Emit(1)
})
var sumReducer = gio.RegisterReducer(func(x, y interface{}) (interface{}, error) {
	return gio.ToInt64(x) + gio.ToInt64(y), nil
})

func projectUsingMoreThanOneLanguages(path string, count *int) *flow.Dataset {
	fmt.Println(">>> Projects and number of languages:")
	return getBlobs("projectUsingMoreThanOneLanguages", path).
		Map("classifyLanguage", classifyLanguageMapper).
		GroupBy("group by repo", flow.Field(1)).
		Map("count langs", countLangsMapper).
		Map("filter more", filterMoreOneLang).
		Printlnf("repo: %s langs: %d").
		Map("to one", toOneMapper).
		Reduce("sum", sumReducer).
		SaveFirstRowTo(count)
}

var filterJSMapper = gio.RegisterMapper(func(x []interface{}) error {
	if x[7].(string) != "JavaScript" {
		return nil
	}
	return gio.Emit(x...)
})

func projectsUsingALanguage(path string, count *int) *flow.Dataset {
	lang := "JavaScript"

	fmt.Printf(">>> Projects using language %s:\n", lang)
	return getBlobs("projectsUsingALanguage", path).
		Map("classifyLanguage", classifyLanguageMapper).
		Map("filterJS", filterJSMapper).
		GroupBy("group by repo", flow.Field(1)).
		Select("repo", flow.Field(1)).
		Printlnf("%s").
		Map("to one", toOneMapper).
		Reduce("sum", sumReducer).
		SaveFirstRowTo(count)
}
