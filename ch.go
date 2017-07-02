// Package ch implements
// classic consistent hashing.
package ch

import (
	"crypto/md5"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

var (
	errRingEmpty = errors.New("Hash ring is empty")
)

// Ring implements consistent hashing.
type Ring struct {
	sync.RWMutex
	VNodes int
	nodes  nodeList
}

// node represents a real node
// with an ID and Name association.
type node struct {
	ID   int
	Name string
}

type nodeList []*node

// Satisfy the sort interface for nodeList.

func (n nodeList) Len() int           { return len(n) }
func (n nodeList) Less(i, j int) bool { return n[i].ID < n[j].ID }
func (n nodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

type Config struct {
	VNodes int
}

func New(c *Config) (*Ring, error) {
	if c.VNodes == 0 {
		c.VNodes = 3
	}

	return &Ring{
		VNodes: c.VNodes,
	}, nil
}

func (r *Ring) AddNode(name string) {
	r.Lock()

	for i := 0; i < r.VNodes; i++ {
		key := hashKey(name)
		r.nodes = append(r.nodes, &node{ID: key, Name: name})
	}

	sort.Sort(r.nodes)

	r.Unlock()
}

func (r *Ring) RemoveNode(name string) {
	r.Lock()

	newNodes := r.nodes[:0]
	for _, n := range r.nodes {
		if n.Name != name {
			newNodes = append(newNodes, n)
		}
	}

	r.nodes = newNodes

	r.Unlock()
}

func (r *Ring) GetNode(k string) (string, error) {
	if len(r.nodes) == 0 {
		return "", errRingEmpty
	}

	node := r.search(hashKey(k))

	return node, nil
}

func (r *Ring) search(n int) string {
	r.RLock()

	i := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].ID >= n
	})

	node := r.nodes[i%len(r.nodes)].Name

	r.RUnlock()

	return node
}

func hashKey(s string) int {
	h := fmt.Sprintf("%x", md5.Sum([]byte(s)))
	k, _ := strconv.ParseInt(h, 16, 32)

	return int(k)
}
