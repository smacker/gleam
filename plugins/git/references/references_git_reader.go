package references

import (
	"strconv"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type Reader struct {
	repositoryID string
	repo         *git.Repository
	refs         storer.ReferenceIter
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references from repository")
	}
	return &Reader{
		repositoryID: path,
		repo:         repo,
		refs:         refs,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"refHash",
		"refName",
		"commitHash",
		"isRemote",
	}, nil
}

func (r *Reader) Read() (*util.Row, error) {
	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	// Get correct commit hash
	// there is Repository.ResolveRevision but it fails on some tags and performance is worst

	commitHash := ref.Hash()
	// handle symbolic references like HEAD
	if ref.Type() == plumbing.SymbolicReference {
		targetRef, _ := r.repo.Reference(ref.Target(), true)
		commitHash = targetRef.Hash()
	}

	// handle tag references
	tag, err := r.repo.TagObject(commitHash)
	if err == nil {
		commit, _ := tag.Commit()
		commitHash = commit.Hash
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		ref.Hash().String(),
		ref.Name().String(),
		commitHash.String(),
		strconv.FormatBool(ref.Name().IsRemote()),
	), nil
}
