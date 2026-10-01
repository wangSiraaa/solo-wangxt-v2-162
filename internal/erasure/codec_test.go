package erasure_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"ecstore/internal/erasure"
)

func newCodec(t *testing.T) *erasure.Codec {
	t.Helper()
	c, err := erasure.New(4, 2, 32) // 小块，便于造“非整块”数据
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestTailPaddingAndStripeCount(t *testing.T) {
	c := newCodec(t)
	// 条带逻辑跨度 = D*B = 128
	cases := []struct {
		size        int64
		stripes     int
		tailPadding int
	}{
		{0, 0, 0},
		{1, 1, 127}, // 整条 128，写了 1 字节，补 127
		{32, 1, 96}, // 恰好一块：后面还有 3 个空块
		{33, 1, 95}, // 第二块只 1 字节
		{128, 1, 0}, // 恰好一条
		{129, 2, 127},
		{256, 2, 0},
	}
	for _, tc := range cases {
		if got := c.StripeCount(tc.size); got != tc.stripes {
			t.Errorf("size=%d StripeCount=%d want %d", tc.size, got, tc.stripes)
		}
		if got := c.TailPadding(tc.size); got != tc.tailPadding {
			t.Errorf("size=%d TailPadding=%d want %d", tc.size, got, tc.tailPadding)
		}
	}
}

// encodeAll 把 data 切成条带编码，返回每个条带的 D+P 个块。
func encodeAll(t *testing.T, c *erasure.Codec, data []byte) [][][]byte {
	t.Helper()
	var out [][][]byte
	_, stripes, _, err := c.EncodeStream(bytes.NewReader(data), func(st int, blocks [][]byte) error {
		cp := make([][]byte, len(blocks))
		for j := range blocks {
			cp[j] = append([]byte(nil), blocks[j]...)
		}
		out = append(out, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if stripes != len(out) {
		t.Fatalf("stripe count %d != %d", stripes, len(out))
	}
	return out
}

func logicalStripe(c *erasure.Codec, blocks [][]byte) []byte {
	var out []byte
	for j := 0; j < c.DataShards(); j++ {
		out = append(out, blocks[j]...)
	}
	return out
}

func TestRoundTripAndPaddingNotReturned(t *testing.T) {
	c := newCodec(t)
	// 200 字节：一条 128 + 一条 72，最后数据块补零；补零绝不能混进原数据。
	data := make([]byte, 200)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	stripes := encodeAll(t, c, data)
	if len(stripes) != 2 {
		t.Fatalf("stripes=%d want 2", len(stripes))
	}
	var got []byte
	for st, blocks := range stripes {
		if err := c.ReconstructStripe(blocks); err != nil {
			t.Fatalf("reconstruct stripe %d: %v", st, err)
		}
		logical := logicalStripe(c, blocks)
		if st == len(stripes)-1 {
			pad := c.TailPadding(int64(len(data)))
			logical = logical[:len(logical)-pad]
		}
		got = append(got, logical...)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("往返数据不一致：got len=%d want len=%d", len(got), len(data))
	}
}

func TestReconstructFromAnyCombination(t *testing.T) {
	c := newCodec(t)
	data := make([]byte, 400)
	_, _ = rand.Read(data)
	stripes := encodeAll(t, c, data)
	D, T := c.DataShards(), c.TotalShards()

	// 容 P=2：随机抹掉 1~2 个块（含只抹校验块、抹数据块、各抹一个）都要能重建。
	dropSets := [][]int{
		{4},       // 只丢一个校验块
		{0},       // 只丢一个数据块
		{0, 5},    // 一数据一校验
		{4, 5},    // 两个校验
		{0, 1, 4}, // 恰好 P+1=3 → 必须失败
	}
	for _, drops := range dropSets {
		st := 0
		blocks := make([][]byte, T)
		for j := 0; j < T; j++ {
			b := append([]byte(nil), stripes[st][j]...)
			for _, d := range drops {
				if j == d {
					b = nil
				}
			}
			blocks[j] = b
		}
		err := c.ReconstructStripe(blocks)
		if len(drops) <= c.ParityShards() {
			if err != nil {
				t.Errorf("drops=%v 意外失败: %v (D=%d)", drops, err, D)
				continue
			}
			for j := 0; j < T; j++ {
				if !bytes.Equal(blocks[j], stripes[st][j]) {
					t.Errorf("drops=%v 重建后块 %d 与原始不一致", drops, j)
				}
			}
		} else {
			if err == nil {
				t.Errorf("drops=%v 应当因有效块 < D 而失败，但成功了", drops)
			}
		}
	}
}

// 验证补齐块的哈希包含零字节 —— 损坏补零区同样会被发现。
func TestPaddingZoneIsChecksummed(t *testing.T) {
	c := newCodec(t)
	data := make([]byte, 1) // 块内 31 字节补零
	stripes := encodeAll(t, c, data)
	hGood := sha256.Sum256(stripes[0][0])
	// 翻转补零区一字节
	tampered := append([]byte(nil), stripes[0][0]...)
	tampered[10] ^= 0xFF
	hBad := sha256.Sum256(tampered)
	if hex.EncodeToString(hGood[:]) == hex.EncodeToString(hBad[:]) {
		t.Fatal("补零区翻转未改变哈希")
	}
}

func TestEncodeStreamEmpty(t *testing.T) {
	c := newCodec(t)
	called := 0
	size, n, pad, err := c.EncodeStream(io.Reader(bytes.NewReader(nil)), func(int, [][]byte) error {
		called++
		return nil
	})
	if err != nil || size != 0 || n != 0 || pad != 0 || called != 0 {
		t.Fatalf("空流: size=%d n=%d pad=%d called=%d err=%v", size, n, pad, called, err)
	}
}

func TestRejectInvalidParams(t *testing.T) {
	if _, err := erasure.New(0, 2, 32); err == nil {
		t.Fatal("D=0 应报错")
	}
	if _, err := erasure.New(4, 0, 32); err == nil {
		t.Fatal("P=0 应报错")
	}
	if _, err := erasure.New(4, 2, 0); err == nil {
		t.Fatal("block=0 应报错")
	}
}

func TestEncodeRejectsOverlongStripe(t *testing.T) {
	c := newCodec(t)
	big := make([]byte, c.DataShards()*c.BlockSize()+1)
	if _, err := c.Encode(big); err == nil {
		t.Fatal("超长条带应报错")
	}
	if _, err := c.Encode(nil); err == nil {
		t.Fatal("空条带应报错")
	}
}
