package git

import (
	"fmt"

	"github.com/chrislusf/gleam/plugins/git/readers"
	"github.com/chrislusf/gleam/plugins/git/repositories"
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

func Repositories(path string, partitionCount int) *sourceRepositories {
	return newGitRepositories(path, partitionCount)
}

type reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}

func (ds *shardInfo) NewReader(r *git.Repository, path string, flag bool) (reader, error) {
	if ds.DataType == "repositories" {
		return repositories.NewReader(r, path)
	}

	refsReader, err := readers.NewReferences(r, path, ds.FilterRefs)
	if err != nil {
		return nil, err
	}

	if ds.DataType == "references" {
		return refsReader, nil
	}

	refs, err := refsReader.GetIter()
	if err != nil {
		return nil, err
	}

	commitsReader, err := readers.NewCommits(r, path, refs, ds.AllCommits)
	if err != nil {
		return nil, err
	}

	if ds.DataType == "commits" {
		return commitsReader, nil
	}

	treesReader, err := readers.NewTrees(r, path, commitsReader.GetIter())
	if err != nil {
		return nil, err
	}

	if ds.DataType == "trees" {
		return treesReader, nil
	}

	return nil, fmt.Errorf("unkown data type %q", ds.DataType)
}
