package commits

import (
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Reader struct {
	repositoryID string
	commits      object.CommitIter
	refs         map[string]struct{}
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references for repository")
	}
	commits, err := repo.CommitObjects()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch commit objects for repository")
	}

	// References even in very large projects are limited enough
	// that they can be stored and kept in memory when building
	// the commits DataSource
	m := make(map[string]struct{})
	var e struct{}

	refs.ForEach(func(ref *plumbing.Reference) error {
		m[ref.Hash().String()] = e
		return nil
	})

	return &Reader{
		repositoryID: path,
		commits:      commits,
		refs:         m,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"commitHash",
		"treeHash",
		"parentHashes",
		"parentsCount",
		"message",
		"authorEmail",
		"authorName",
		"authorDate",
		"committerEmail",
		"committerName",
		"committerDate",
	}, nil
}

func (r *Reader) Read() (*util.Row, error) {
	commit, err := r.commits.Next()
	if err != nil {
		// do not wrap this error, as it could be an io.EOF.
		return nil, err
	}

	var parentHashes []string
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		commit.Hash.String(),
		commit.TreeHash.String(),
		parentHashes,
		len(parentHashes),
		commit.Message,
		commit.Author.Email,
		commit.Author.Name,
		commit.Author.When.Unix(),
		commit.Committer.Email,
		commit.Committer.Name,
		commit.Committer.When.Unix(),
	), nil
}
