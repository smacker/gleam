package trees

import (
	"errors"
	"io"
	"strconv"

	"gopkg.in/src-d/go-git.v4/plumbing"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreesGitReader struct {
	repositoryID string
	repo         *git.Repository
	treeIter     *object.TreeIter
	fileIter     *object.FileIter
	hashes       []string
	lastTreeHash string
}

func New(r *git.Repository, path string, hashes []string) *TreesGitReader {
	treeIter, _ := r.TreeObjects()

	return &TreesGitReader{
		repositoryID: path,
		repo:         r,
		treeIter:     treeIter,
		hashes:       hashes,
	}
}

func (r *TreesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"blobHash",
		"fileName",
		"treeHash",
		"blobSize",
		"isBinary",
	}

	return fieldNames, nil
}

func (r *TreesGitReader) Read() (row *util.Row, err error) {
	if r.fileIter == nil {
		tree, err := r.treeIter.Next()
		if err != nil {
			return nil, errors.New("end of treeIter")
		}
		r.lastTreeHash = tree.Hash.String()
		r.fileIter = tree.Files()
	}

	file, err := r.fileIter.Next()
	if err != nil {
		r.fileIter = nil
		return nil, nil
	}

	binary, _ := file.IsBinary()

	return util.NewRow(util.Now(),
		r.repositoryID,
		file.Blob.Hash.String(),
		file.Name,
		r.lastTreeHash,
		file.Blob.Size,
		strconv.FormatBool(binary),
	), nil
}

func (r *TreesGitReader) ByHashes() (row *util.Row, err error) {
	if r.fileIter == nil {
		if len(r.hashes) == 0 {
			return nil, io.EOF
		}

		hashString := r.hashes[0]
		r.hashes = r.hashes[1:]
		h := plumbing.NewHash(hashString)
		tree, err := r.repo.TreeObject(h)
		if err != nil {
			return nil, err
		}

		r.lastTreeHash = tree.Hash.String()
		r.fileIter = tree.Files()
	}

	file, err := r.fileIter.Next()
	if err != nil {
		r.fileIter = nil
		return nil, nil
	}

	binary, _ := file.IsBinary()

	return util.NewRow(util.Now(),
		r.repositoryID,
		file.Blob.Hash.String(),
		file.Name,
		r.lastTreeHash,
		file.Blob.Size,
		strconv.FormatBool(binary),
	), nil
}
