package repositories

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type RepositoriesGitReader struct {
	repo         *git.Repository
	path         string
	repositoryID string
	urls         []string
	refs         storer.ReferenceIter
	head         *plumbing.Reference
	hasBeenRead  bool
}

func New(r *git.Repository, path string) *RepositoriesGitReader {

	refs, _ := r.References()
	remotes, _ := r.Remotes()
	head, _ := r.Head()
	urls := remotes[0].Config().URLs

	return &RepositoriesGitReader{
		repo:         r,
		repositoryID: path,
		urls:         urls,
		refs:         refs,
		head:         head,
	}
}

func (r *RepositoriesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"repositoryPath",
		"repositoryURLs",
		"headHash",
	}
	return fieldNames, nil
}

//TODO: add is_fork
func (r *RepositoriesGitReader) Read() (row *util.Row, err error) {
	if r.hasBeenRead {
		return nil, io.EOF
	}
	r.hasBeenRead = true
	return util.NewRow(util.Now(), r.repositoryID, r.urls, r.head.Hash().String()), nil
}
