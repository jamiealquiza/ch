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

// Ring implmenents a consistent hashing
// ring with a configurable number of vnodes.
type Ring struct {
	sync.RWMutex
	Vnodes int
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

func (h *Ring) AddNode(name string) {
	h.Lock()

	for i := 0; i < h.Vnodes; i++ {
		key := hashKey(name)
		h.nodes = append(h.nodes, &node{ID: key, Name: name})
	}

	sort.Sort(h.nodes)

	h.Unlock()
}

func (h *Ring) RemoveNode(name string) {
	h.Lock()

	newNodes := []*node{}
	for _, n := range h.nodes {
		if n.Name != name {
			newNodes = append(newNodes, n)
		}
	}

	h.nodes = newNodes

	h.Unlock()
}

func (h *Ring) GetNode(k string) (string, error) {
	if len(h.nodes) == 0 {
		return "", errors.New("Hash ring is empty")
	}

	h.RLock()

	hk := hashKey(k)
	i := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i].ID >= hk }) % len(h.nodes)
	node := h.nodes[i].Name

	h.RUnlock()

	return node, nil
}

func hashKey(s string) int {
	h := fmt.Sprintf("%x", md5.Sum([]byte(s)))
	k, _ := strconv.ParseInt(h, 16, 32)

	return int(k)
}
