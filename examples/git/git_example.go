package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"

	"gopkg.in/bblfsh/client-go.v2"
	protocol "gopkg.in/bblfsh/sdk.v1/protocol"
	enry "gopkg.in/src-d/enry.v1"
	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")

	regKeyRefHash    = gio.RegisterMapper(flipKey(3))
	regKeyCommitHash = gio.RegisterMapper(flipKey(1))
	regKeyTreeHash   = gio.RegisterMapper(flipKey(1))

	regReadBlob         = gio.RegisterMapper(readBlob)
	regClassifyLanguage = gio.RegisterMapper(classifyLanguage(2, 6))
	regExtractUAST      = gio.RegisterMapper(extractUAST)
)

var (
	flip1 = gio.RegisterMapper(flipKey(1))
	flip2 = gio.RegisterMapper(flipKey(2))
)

func run(p *flow.Flow) {
	if *isDistributed {
		p.Run(distributed.Option())
	} else if *isDockerCluster {
		p.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		p.Run()
	}
}

func main() {
	gio.Init()

	f := flow.New("Git pipeline")
	path := "/Users/smacker/Dev/**"

	headHashes := make(map[string][]string)
	f.Read(git.Repositories(path, 1)).
		Select("head", flow.Field(1, 2)).
		OutputRow(func(row *util.Row) error {
			r := row.K[0].(string)
			v := row.V[0].(string)
			headHashes[r] = append(headHashes[r], v)
			return nil
		})

	run(f)

	treeHashes := make(map[string][]string)
	f.Read(git.Commits(path, 1).Where(headHashes)).
		Select("threeHash", flow.Field(1, 3)).
		OutputRow(func(row *util.Row) error {
			r := row.K[0].(string)
			v := row.V[0].(string)
			treeHashes[r] = append(treeHashes[r], v)
			return nil
		})

	run(f)

	f.Read(git.Trees(path, 1).Where(treeHashes)).Select("name", flow.Field(3)).Printlnf("%s")

	run(f)
}

func flipKey(newKeyIdx int) gio.Mapper {
	return func(x []interface{}) error {
		newKey := make([]interface{}, 1)
		newKey[0] = x[newKeyIdx]
		row := x[:newKeyIdx]
		if len(x) > newKeyIdx+1 {
			row = append(row, x[newKeyIdx+1:]...)
		}
		row = append(newKey, row...)
		gio.Emit(row...)
		return nil
	}
}

//TODO: Update to new index approach
func readBlob(x []interface{}) error {
	repoPath := gio.ToString(x[1])
	blobHash := plumbing.NewHash(gio.ToString(x[5]))
	contents := []byte("")

	if !blobHash.IsZero() {
		r, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return err
		}

		blob, err := r.BlobObject(blobHash)
		if err != nil {
			return err
		}

		reader, err := blob.Reader()
		if err != nil {
			return err
		}

		contents, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
	}

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], contents)
	return nil
}

func classifyLanguage(fileNameIdx int, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[fileNameIdx])
		content := x[contentIdx].([]byte)
		lang := enry.GetLanguage(filename, content)
		gio.Emit(append(x, lang)...)
		return nil
	}
}

//TODO: Update to new index approach
func extractUAST(x []interface{}) error {
	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		panic(err)
	}

	blob := gio.ToString(x[4])

	res, err := client.NewParseRequest().Language("python").Content(blob).Do()
	if err != nil {
		panic(err)
	}

	if res.Response.Status == protocol.Fatal {
		res.Language = ""
	}

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], res.Language)
	return nil
}

func toPrint(val interface{}) string {
	return fmt.Sprintf("%v", val)
}

func truncateString(str string, num int) string {
	b := str
	if len(str) > num {
		if num > 3 {
			num -= 3
		}
		b = str[0:num] + "..."
	}
	return b
}
