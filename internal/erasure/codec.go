// Package erasure 封装 klauspost/reedsolomon 的条带编解码。
//
// 布局说明（教学项目）：
//   - 对象被切成固定长度的“条带(stripe)”，每个条带长 D*BlockSize。
//   - 一个条带又均分为 D 个数据块，经 RS 编码产生 P 个校验块。
//   - 每个节点 j 的分片文件 = 所有条带的第 j 块顺序拼接。
//   - 最后一个数据块不足 BlockSize 时，块内补零（zero pad），
//     对象总长度记录在清单里，读取时按长度截断 —— 绝不把补零当原内容返回。
package erasure

import (
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

// Codec 对固定 (D, P, BlockSize) 拓扑做条带编解码。
type Codec struct {
	enc       reedsolomon.Encoder
	dataN     int
	parityN   int
	blockSize int
}

func New(dataN, parityN, blockSize int) (*Codec, error) {
	if dataN <= 0 || parityN <= 0 || blockSize <= 0 {
		return nil, fmt.Errorf("无效的纠删码参数 d=%d p=%d block=%d", dataN, parityN, blockSize)
	}
	enc, err := reedsolomon.New(dataN, parityN)
	if err != nil {
		return nil, err
	}
	return &Codec{enc: enc, dataN: dataN, parityN: parityN, blockSize: blockSize}, nil
}

func (c *Codec) DataShards() int   { return c.dataN }
func (c *Codec) ParityShards() int { return c.parityN }
func (c *Codec) BlockSize() int    { return c.blockSize }
func (c *Codec) TotalShards() int  { return c.dataN + c.parityN }

// StripeCount 返回 size 字节对象占用的条带数（空对象为 0）。
func (c *Codec) StripeCount(size int64) int {
	stride := int64(c.dataN * c.blockSize)
	if size <= 0 {
		return 0
	}
	return int((size + stride - 1) / stride)
}

// TailPadding 返回最后一个条带为凑满 D*BlockSize 而补齐的零字节数。
// 这既包含最后一个数据块内部的补零，也包含整对象在整条层面缺失的若干空块
// （例如 D=4、只写了 2.5 块时，最后 1.5 块全是补零）。
// 注意：补零仅用于满足 RS 编码“块等长”的要求，不属于对象内容。
func (c *Codec) TailPadding(size int64) int {
	if size <= 0 {
		return 0
	}
	stride := int64(c.dataN * c.blockSize)
	rem := size % stride
	if rem == 0 {
		return 0
	}
	return int(stride - rem)
}

// Encode 读取完整一个条带（可能是最后一个短条带），返回 D+P 个等长块。
// data 的长度必须 <= D*BlockSize 且 > 0；缺口以零填充。
// 每次调用都返回一块全新的底层数组，调用方可以安全持有/落盘。
func (c *Codec) Encode(data []byte) ([][]byte, error) {
	capLen := c.dataN * c.blockSize
	if len(data) == 0 || len(data) > capLen {
		return nil, fmt.Errorf("条带数据长度非法: %d (允许 1..%d)", len(data), capLen)
	}
	buf := make([]byte, c.TotalShards()*c.blockSize)
	copy(buf, data) // 末尾缺口天然为 0，即 zero pad

	blocks := make([][]byte, c.TotalShards())
	for j := range blocks {
		blocks[j] = buf[j*c.blockSize : (j+1)*c.blockSize]
	}
	if err := c.enc.Encode(blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

// ReconstructStripe 对一个条带做重建。blocks 长度 D+P，缺失/损坏的块传 nil。
// 当可用块少于 D 时返回 reedsolomon 的 ErrTooFewShards（调用方据此判定不可恢复）。
// 成功后 blocks 中所有 nil 位置都会被填充 —— 注意必须用 Reconstruct
// （而不是 ReconstructData）：后者只补数据分片，校验分片会保持 nil/错误内容。
func (c *Codec) ReconstructStripe(blocks [][]byte) error {
	if len(blocks) != c.TotalShards() {
		return fmt.Errorf("块数量 %d != %d", len(blocks), c.TotalShards())
	}
	// 防御：保证非 nil 块长度恰好为 BlockSize。
	for j, b := range blocks {
		if b == nil {
			continue
		}
		if len(b) != c.blockSize {
			blocks[j] = nil // 长度异常等同于损坏
		}
	}
	return c.enc.Reconstruct(blocks)
}

// EncodeStream 把 r 的全部内容按条带编码，每编码出一个条带就回调 onStripe
// （stripe 从 0 开始；blocks 在回调返回后会被复用，回调方需要自行拷贝/落盘）。
// 返回对象逻辑长度、总条带数与尾部补零长度。
func (c *Codec) EncodeStream(r io.Reader, onStripe func(stripe int, blocks [][]byte) error) (size int64, stripes int, tailPad int, err error) {
	stride := c.dataN * c.blockSize
	buf := make([]byte, stride)
	idx := 0
	for {
		n, readErr := io.ReadFull(r, buf[idx:])
		idx += n
		size += int64(n)
		switch {
		case readErr == nil:
			// 读满整条
		case readErr == io.ErrUnexpectedEOF:
			// 最后一条短数据：idx > 0，缺口在 Encode 内补零
		case readErr == io.EOF:
			// 流恰好在条带边界结束（idx 必为 0）
			return size, stripes, c.TailPadding(size), nil
		default:
			return size, stripes, 0, readErr
		}

		blocks, encErr := c.Encode(buf[:idx])
		if encErr != nil {
			return size, stripes, 0, encErr
		}
		if err := onStripe(stripes, blocks); err != nil {
			return size, stripes, 0, err
		}
		stripes++
		if readErr == io.ErrUnexpectedEOF {
			return size, stripes, c.TailPadding(size), nil
		}
		idx = 0
	}
}
