package references

import (
	"io"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type iterator struct {
	repo     *git.Repository
	refNames []plumbing.ReferenceName
	pos      int
}

func (iter *iterator) Next() (*plumbing.Reference, error) {
	if iter.pos >= len(iter.refNames) {
		return nil, io.EOF
	}
	refName := iter.refNames[iter.pos]
	ref, err := iter.repo.Reference(refName, true)
	if err != nil {
		return nil, err
	}
	iter.pos++
	return ref, nil
}

// ForEach call the cb function for each reference contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stopped but no error is returned. The iterator is closed.
func (iter *iterator) ForEach(cb func(*plumbing.Reference) error) error {
	defer iter.Close()
	for {
		r, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cb(r); err != nil {
			if err == storer.ErrStop {
				break
			}

			return err
		}
	}

	return nil
}

func (iter *iterator) Close() {}
