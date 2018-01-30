package references

import (
	"errors"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type ReferencesGitReader struct {
	repositoryID string
	refs         storer.ReferenceIter
}

func New(r *git.Repository, path string, hashes []string) *ReferencesGitReader {

	refs, _ := r.References()
	return &ReferencesGitReader{
		repositoryID: path,
		refs:         refs,
	}
}

func (r *ReferencesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"refHash",
		"refName",
		"commitHash",
	}
	return fieldNames, nil
}

//TODO: add is_remote
func (r *ReferencesGitReader) Read() (row *util.Row, err error) {

	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, ref.Hash().String(), ref.Name().String()), nil
}

func (r *ReferencesGitReader) ByHashes() (row *util.Row, err error) {
	return nil, errors.New("not implemented")
}
