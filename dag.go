package merkledag

import (
	"encoding/json"
	"hash"
)

const BlobSize = 262144

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {

	if node.Type() == FILE {
		file := node.(File)
		fileSlice := addBlock(file.Bytes(), store, h)
		jsonData, _ := json.Marshal(fileSlice)
		h.Write(jsonData)
		return h.Sum(nil)
	} else {
		dir := node.(Dir)
		dirSlice := addDir(dir, store, h)
		jsonData, _ := json.Marshal(dirSlice)
		h.Write(jsonData)
		return h.Sum(nil)
	}
}

func addDir(store KVStore, node Dir, h hash.Hash) *Object {
	iter := node.It()
	tree := &Object{}
	for iter.Next() {
		elem := iter.Node()
		if elem.Type() == FILE {
			file := elem.(File)
			fileSlice := addBlock(file.Bytes(), store, h)
			jsonData, _ := json.Marshal(fileSlice)
			h.Reset()
			h.Write(jsonData)
			tree.Links = append(tree.Links, Link{
				Hash: h.Sum(nil),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			elemType := "link"
			if fileSlice.Links == nil {
				elemType = "data"
			}
			tree.Data = append(tree.Data, []byte(elemType)...)
		} else {
			dir := elem.(Dir)
			dirSlice := addDir(dir, store, h)
			jsonData, _ := json.Marshal(dirSlice)
			h.Reset()
			h.Write(jsonData)
			tree.Links = append(tree.Links, Link{
				Hash: h.Sum(nil),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			elemType := "tree"
			tree.Data = append(tree.Data, []byte(elemType)...)
		}
	}
	jsonData, _ := json.Marshal(tree)
	h.Reset()
	h.Write(jsonData)
	exists, _ := store.Has(h.Sum(nil))
	if !exists {
		store.Put(h.Sum(nil), jsonData)
	}
	return tree
}

func addBlock(data []byte, store KVStore, h hash.Hash) Object {
	slips := ChunkData(data, BlobSize)
	var links []Link
	var Size = 0
	for i := range slips {
		_, blobHash := addSingleBlob(slips[i], store, h)
		links = append(links, Link{
			Name: string(blobHash),
			Hash: blobHash,
			Size: len(slips[i]),
		})
		Size += len(slips[i])
	}
	res := Object{
		Links: links,
		Data:  nil,
	}
	marshal, err := json.Marshal(res)
	if err != nil {
		return Object{nil, nil}
	}
	has := h.Sum(marshal)
	ifhave, err := store.Has(has)
	if err != nil {
		return Object{}
	}
	if !ifhave {
		store.Put(has, marshal)
	}
	return res
}

// ChunkData 分片
func ChunkData(data []byte, chunkSize int) [][]byte {
	var chunks [][]byte

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]
		chunks = append(chunks, chunk)
	}

	return chunks
}

func addSingleBlob(data []byte, store KVStore, h hash.Hash) (Object, []byte) {
	bytes := h.Sum(data)
	has, err := store.Has(bytes)
	if err != nil {
		return Object{}, nil
	}
	if !has {
		err := store.Put(bytes, data)
		if err != nil {
			return Object{}, nil
		}
	}
	return Object{Data: data}, bytes
}
