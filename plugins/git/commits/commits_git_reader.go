package commits

import (
	"io"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type CommitsGitReader struct {
	repositoryID string
	repo         *git.Repository
	commits      object.CommitIter
	refs         map[string]struct{}
	hashes       []string
}

func New(r *git.Repository, path string, hashes []string) *CommitsGitReader {

	refs, _ := r.References()
	commits, _ := r.CommitObjects()

	// References even in very large projects are limited enough
	// that they can be stored and kept in memory when building
	// the commits DataSource
	m := make(map[string]struct{})
	var e struct{}

	refs.ForEach(func(ref *plumbing.Reference) error {
		m[ref.Hash().String()] = e
		return nil
	})

	return &CommitsGitReader{
		repositoryID: path,
		repo:         r,
		commits:      commits,
		refs:         m,
		hashes:       hashes,
	}
}

func (r *CommitsGitReader) ReadHeader() (fieldNames []string, err error) {

	fieldNames = []string{
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
	}

	return fieldNames, nil
}

func (r *CommitsGitReader) Read() (row *util.Row, err error) {

	commit, err := r.commits.Next()
	if err != nil {
		return nil, err
	}

	commitHash := commit.Hash.String()
	message := commit.Message
	treeHash := commit.TreeHash.String()

	var parentHashes []string
	var parentsCount int
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
		parentsCount++
	}

	authorEmail := commit.Author.Email
	authorName := commit.Author.Name
	authorDate := commit.Author.When.Unix()
	committerEmail := commit.Committer.Email
	committerName := commit.Committer.Name
	committerDate := commit.Committer.When.Unix()

	return util.NewRow(util.Now(),
		r.repositoryID,
		commitHash,
		treeHash,
		parentHashes,
		parentsCount,
		message,
		authorEmail,
		authorName,
		authorDate,
		committerEmail,
		committerName,
		committerDate,
	), nil
}

func (r *CommitsGitReader) ByHashes() (row *util.Row, err error) {
	if len(r.hashes) == 0 {
		return nil, io.EOF
	}

	hashString := r.hashes[0]
	r.hashes = r.hashes[1:]
	h := plumbing.NewHash(hashString)

	commit, err := r.repo.CommitObject(h)
	if err != nil {
		return nil, err
	}

	commitHash := commit.Hash.String()
	message := commit.Message
	treeHash := commit.TreeHash.String()

	var parentHashes []string
	var parentsCount int
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
		parentsCount++
	}

	authorEmail := commit.Author.Email
	authorName := commit.Author.Name
	authorDate := commit.Author.When.Unix()
	committerEmail := commit.Committer.Email
	committerName := commit.Committer.Name
	committerDate := commit.Committer.When.Unix()

	return util.NewRow(util.Now(),
		r.repositoryID,
		commitHash,
		treeHash,
		parentHashes,
		parentsCount,
		message,
		authorEmail,
		authorName,
		authorDate,
		committerEmail,
		committerName,
		committerDate,
	), nil
}
