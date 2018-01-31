package git

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

type shardInfo struct {
	// these fields are exported so gob encoding can see them.
	Config    map[string]string
	RepoPath  string
	DataType  string
	HasHeader bool
	Fields    []string
	Optimize  bool
}

var registeredMapperReadShard = gio.RegisterMapper(readShard)

func init() {
	gob.Register(shardInfo{})
}

func readShard(row []interface{}) error {
	var s shardInfo
	if err := s.decode(gio.ToBytes(row[0])); err != nil {
		return err
	}
	return s.ReadSplit()
}

func removeDuplicates(vs []string) []string {
	result := make([]string, 0, len(vs))
	seen := make(map[string]string, len(vs))
	for _, v := range vs {
		if _, ok := seen[v]; !ok {
			result = append(result, v)
			seen[v] = v
		}
	}
	return result
}

func getRefChildren(path, refHash string, refName string) error {
	cmd := exec.Command("git", "rev-list", "--children", refHash)
	cmd.Dir = path
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "could not run %s %s", cmd.Path, cmd.Args)
	}

	hashes := removeDuplicates(strings.Fields(string(output)))
	for _, hash := range hashes {
		row := util.NewRow(util.Now(), path, refHash, refName, hash)
		row.WriteTo(os.Stdout)
	}
	return nil
}

func (s *shardInfo) ReadSplit() error {
	log.Printf("reading %s from repo: %s", s.DataType, s.RepoPath)

	repo, err := git.PlainOpen(s.RepoPath)
	if err != nil {
		return errors.Wrap(err, "could not open repo")
	}

	reader, err := s.NewReader(repo, s.RepoPath, s.Optimize)
	if err != nil {
		return errors.Wrapf(err, "could not read repository %s", s.RepoPath)
	}
	if s.HasHeader {
		if _, err := reader.ReadHeader(); err != nil {
			return errors.Wrap(err, "could not read headers")
		}
	}

	if s.DataType == "references" {
		refs, err := repo.References()
		if err != nil {
			return errors.Wrap(err, "could not read references")
		}
		return refs.ForEach(func(ref *plumbing.Reference) error {
			if ref.Hash().IsZero() {
				return nil
			}
			return getRefChildren(s.RepoPath, ref.Hash().String(), ref.Name().String())
		})
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "could not read")
		}

		// Writing to stdout is how agents communicate.
		if err := row.WriteTo(os.Stdout); err != nil {
			return errors.Wrap(err, "could not write row to stdout")
		}
	}
}

func (s *shardInfo) decode(b []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(b))
	return dec.Decode(s)
}

func (s *shardInfo) encode() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(s); err != nil {
		return nil, errors.Wrap(err, "could not encode shard info")
	}
	return b.Bytes(), nil
}
