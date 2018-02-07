package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"

	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

var isDistributed = flag.Bool("distributed", false, "run in distributed or not")
var pathPtr = flag.String("path", ".", "")

func main() {
	gio.Init()

	path := *pathPtr
	log.Printf("analyzing %s", path)

	var opts []flow.FlowOption
	if *isDistributed {
		opts = append(opts, distributed.Option())
	}

	var count int
	mostUsedLanguages(path).Run(opts...)

	projectUsingMoreThanOneLanguages(path, &count).Run(opts...)
	fmt.Println("Projects using language more than 1 language: ", count)

	projectsUsingALanguage(path, &count).Run(opts...)
	fmt.Println("Total", count)

	return

	st := time.Now()
	checkAllRefs(path)
	fmt.Printf("All refs: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkFilterRefs(path, "refs/heads/master")
	fmt.Printf("Refs for master:%d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkCommits(path, "refs/heads/master")
	fmt.Printf("Commits for master: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkAllCommits(path, "refs/heads/master")
	fmt.Printf("All commits for master: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkTrees(path, "refs/heads/master")
	fmt.Printf("Trees for master: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkBlobs(path, "refs/heads/master")
	fmt.Printf("Blobs for master: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
	st = time.Now()
	checkAllCommitsTrees(path, "refs/heads/master")
	fmt.Printf("All commits trees: %d\n", count)
	fmt.Println(time.Now().Sub(st))
	count = 0
}

var (
	opts    []flow.FlowOption
	regKey1 = gio.RegisterMapper(columnToKey(1))
	refKey2 = gio.RegisterMapper(columnToKey(1))
	regKey3 = gio.RegisterMapper(columnToKey(3))
)

func checkAllRefs(path string) {
	flow.New("test all refs").
		Read(git.Repositories(path, 1).References()).
		OutputRow(printRow).Run()
}

func checkFilterRefs(path string, refName string) {
	flow.New("test refs").
		Read(git.Repositories(path, 1).References().Filter(refName)).
		OutputRow(printRow).Run()
}

func checkCommits(path string, refName string) {
	f := flow.New("test commits")
	f.Read(git.Repositories(path, 1).References().Filter(refName).Commits()).
		OutputRow(printRow).Run()
}

func checkAllCommits(path string, refName string) {
	f := flow.New("test all commits")
	f.Read(git.Repositories(path, 1).References().Filter(refName).AllReferenceCommits()).
		OutputRow(printRow).Run()
}

func checkTrees(path string, refName string) {
	f := flow.New("test trees")
	f.Read(git.Repositories(path, 1).References().Filter(refName).Commits().Trees()).
		OutputRow(printRow).Run()
}

func checkAllCommitsTrees(path string, refName string) {
	f := flow.New("test trees")
	f.Read(git.Repositories(path, 1).References().Filter(refName).AllReferenceCommits().Trees()).
		OutputRow(printRow).Run()
}

func checkBlobs(path string, refName string) {
	f := flow.New("test trees")
	f.Read(git.Repositories(path, 1).References().Filter(refName).Commits().Trees().Blobs()).
		OutputRow(printRow).Run()
}

var count int64

func printRow(row *util.Row) error {
	//	fmt.Printf("\n\n%v\t", row.K[0])
	count++
	// for _, v := range row.V {
	// 	fmt.Printf("%v\t", v)
	// }
	// fmt.Println()
	return nil
}

func columnToKey(i int) gio.Mapper {
	return func(x []interface{}) error {
		row := append([]interface{}{x[i]}, x[:i]...)
		row = append(row, x[i+1:]...)
		return gio.Emit(row...)
	}
}

func readBlob(repoPathIdx, blobHashIdx int) gio.Mapper {
	return func(x []interface{}) error {
		repoPath := gio.ToString(x[repoPathIdx])
		blobHash := plumbing.NewHash(gio.ToString(x[blobHashIdx]))

		if blobHash.IsZero() {
			return gio.Emit(x[:len(x)+1]...)
		}

		r, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return errors.Wrapf(err, "could not open repo at %s", repoPath)
		}

		blob, err := r.BlobObject(blobHash)
		if err != nil {
			return errors.Wrapf(err, "could not retrieve blob object with hash %s", blobHash)
		}

		reader, err := blob.Reader()
		if err != nil {
			return errors.Wrapf(err, "could not read blob with hash %s", blobHash)
		}

		contents, err := ioutil.ReadAll(reader)
		reader.Close()
		if err != nil {
			return errors.Wrapf(err, "could not fully read blob with hash %s", blobHash)
		}

		return gio.Emit(append(x, contents)...)
	}
}
