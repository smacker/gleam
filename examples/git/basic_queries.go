package main

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/git"
)

func getCommits(name, path string) *flow.Dataset {
	return flow.New(name).Read(git.Repositories(path, 1).References().Commits())
}

func getBlobs(name, path string) *flow.Dataset {
	return flow.New(name).Read(git.Repositories(path, 1).References().Commits().Trees().Blobs())
}
