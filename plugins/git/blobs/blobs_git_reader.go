package blobs

import (
	"errors"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type BlobsGitReader struct {
	repositoryID string
	blobs        *object.BlobIter
}

func New(r *git.Repository, path string, hashes []string) *BlobsGitReader {

	blobs, _ := r.BlobObjects()

	return &BlobsGitReader{
		repositoryID: path,
		blobs:        blobs,
	}
}

func (r *BlobsGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"blobHash",
		"blobSize",
	}
	return fieldNames, nil
}

func (r *BlobsGitReader) Read() (row *util.Row, err error) {

	blob, err := r.blobs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, blob.Hash.String(), blob.Size), nil
}

func (r *BlobsGitReader) ByHashes() (row *util.Row, err error) {
	return nil, errors.New("not implemented")
}
