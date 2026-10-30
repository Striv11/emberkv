package sstable

import (
	"hash/fnv"
	"math"
)

// bloom is a standard bloom filter using double hashing derived from one
// 64-bit FNV-1a hash. Its encoding is the bit array followed by one byte
// holding the probe count k.
type bloom struct {
	bits []byte
	k    uint8
}

func newBloom(nKeys, bitsPerKey int) *bloom {
	nbits := nKeys * bitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	k := int(math.Round(float64(bitsPerKey) * math.Ln2))
	k = max(1, min(k, 30))
	return &bloom{bits: make([]byte, (nbits+7)/8), k: uint8(k)}
}

func bloomHash(key []byte) (h1, h2 uint32) {
	f := fnv.New64a()
	f.Write(key)
	h := f.Sum64()
	return uint32(h), uint32(h>>32) | 1
}

func (b *bloom) add(key []byte) {
	n := uint32(len(b.bits)) * 8
	h1, h2 := bloomHash(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint32(i)*h2) % n
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

// mayContain reports false only if key was definitely never added.
func (b *bloom) mayContain(key []byte) bool {
	n := uint32(len(b.bits)) * 8
	h1, h2 := bloomHash(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint32(i)*h2) % n
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloom) encode() []byte {
	return append(append([]byte(nil), b.bits...), b.k)
}

func decodeBloom(p []byte) (*bloom, error) {
	if len(p) < 2 || p[len(p)-1] == 0 {
		return nil, errCorrupt("bad bloom filter")
	}
	return &bloom{bits: p[:len(p)-1], k: p[len(p)-1]}, nil
}
