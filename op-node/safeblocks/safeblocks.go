package safeblocks

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	gethrpc "github.com/ethereum/go-ethereum/rpc"

	"github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	opsigner "github.com/ethereum-optimism/optimism/op-service/signer"
	opsources "github.com/ethereum-optimism/optimism/op-service/sources"
)

type Engine interface {
	UnsafeL2Head() eth.L2BlockRef
	SafeL2Head() eth.L2BlockRef
	Finalized() eth.L2BlockRef
	SetUnsafeHead(eth.L2BlockRef)
	SetSafeHead(eth.L2BlockRef)
	SetLocalSafeHead(eth.L2BlockRef)
	SetFinalizedHead(eth.L2BlockRef)
	SetCrossUnsafeHead(eth.L2BlockRef)
	TryUpdateEngine(ctx context.Context)
	CommitBlock(ctx context.Context, signed *opsigner.SignedExecutionPayloadEnvelope) error
}

type L2 interface {
	L2BlockRefByHash(ctx context.Context, hash common.Hash) (eth.L2BlockRef, error)
	L2BlockRefByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, error)
}

type Config struct {
	RPC      string
	Interval time.Duration
}

type Client struct {
	log    log.Logger
	cfg    Config
	eng    Engine
	l2     L2
	cancel context.CancelFunc
	fetch  BlockFetcher
	// buildEnvelopeFn builds an ExecutionPayloadEnvelope for the given block number.
	// Defaults to buildEnvelopeByNumber, and is overridden in unit tests.
	buildEnvelopeFn func(ctx context.Context, num uint64) (*eth.ExecutionPayloadEnvelope, bool)
}

func New(cfg Config, log log.Logger, eng Engine, l2 L2) *Client {
	c := &Client{cfg: cfg, log: log.New("module", "safeblocks"), eng: eng, l2: l2}
	c.buildEnvelopeFn = c.buildEnvelopeByNumber
	return c
}

func (c *Client) Start(ctx context.Context) error {
	if c.cfg.RPC == "" {
		return nil
	}
	cctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	raw, err := gethrpc.DialContext(ctx, c.cfg.RPC)
	if err != nil {
		return err
	}
	cli := client.NewBaseRPCClient(raw)
	c.fetch = &rpcFetcher{cli: cli}
	ticker := time.NewTicker(c.cfg.Interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-cctx.Done():
				return
			case <-ticker.C:
				c.tick(cctx)
			}
		}
	}()
	return nil
}

func (c *Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Client) tick(ctx context.Context) {
	if c.fetch == nil {
		return
	}
	// Ensure engine heads are initialized before attempting any advancement.
	if s := c.eng.SafeL2Head(); s.Number == 0 && c.eng.UnsafeL2Head().Number == 0 && c.eng.Finalized().Number == 0 {
		c.log.Debug("Waiting for engine heads to initialize before advancing")
		return
	}
	// Fetch targets first
	safeNum, hasSafe := c.fetch.SafeHeadNumber(ctx)
	finNum, hasFin := c.fetch.FinalizedHeadNumber(ctx)
	// Step finalized by at most one, only when present locally and not beyond safe
	if hasFin {
		c.stepFinalizedOne(ctx, finNum)
	}
	// Step safe by at most one: find common ancestor, then ingest next block
	if hasSafe {
		c.stepSafeOne(ctx, safeNum)
	}
}

func fetchBlockByTag(ctx context.Context, cli client.RPC, tag string) (eth.L2BlockRef, bool) {
	var res struct {
		Hash       common.Hash `json:"hash"`
		Number     string      `json:"number"`
		ParentHash common.Hash `json:"parentHash"`
		Timestamp  string      `json:"timestamp"`
	}
	if err := cli.CallContext(ctx, &res, "eth_getBlockByNumber", tag, false); err != nil {
		return eth.L2BlockRef{}, false
	}
	if res.Hash == (common.Hash{}) {
		return eth.L2BlockRef{}, false
	}
	num, ok := parseHexUint64(res.Number)
	if !ok {
		return eth.L2BlockRef{}, false
	}
	tim, ok := parseHexUint64(res.Timestamp)
	if !ok {
		tim = 0
	}
	return eth.L2BlockRef{Hash: res.Hash, Number: num, ParentHash: res.ParentHash, Time: tim}, true
}

func (c *Client) applySafe(ctx context.Context, ext eth.L2BlockRef) {
	localSafe := c.eng.SafeL2Head()
	if localSafe.Hash == ext.Hash {
		return
	}
	if _, err := c.l2.L2BlockRefByHash(ctx, ext.Hash); err != nil {
		c.log.Debug("Skipping safe update: block not present locally", "num", ext.Number, "hash", ext.Hash)
		return
	}
	// Also advance local unsafe to match, if behind
	if u := c.eng.UnsafeL2Head(); ext.Number > u.Number {
		c.eng.SetUnsafeHead(ext)
	}
	c.eng.SetLocalSafeHead(ext)
	c.eng.SetSafeHead(ext)
	c.eng.TryUpdateEngine(ctx)
}

func (c *Client) applyFinalized(ctx context.Context, ext eth.L2BlockRef) {
	localFin := c.eng.Finalized()
	if localFin.Hash == ext.Hash {
		return
	}
	if _, err := c.l2.L2BlockRefByHash(ctx, ext.Hash); err != nil {
		c.log.Debug("Skipping finalized update: block not present locally", "num", ext.Number, "hash", ext.Hash)
		return
	}
	c.eng.SetFinalizedHead(ext)
	c.eng.TryUpdateEngine(ctx)
}

func (c *Client) applyFinalizedByNumber(ctx context.Context, num uint64) {
	if num == 0 {
		return
	}
	if b, ok := c.fetch.BlockByNumber(ctx, num); ok {
		c.applyFinalized(ctx, b)
	}
}

// stepFinalizedOne advances finalized by at most one block towards targetFinNum.
// It never moves beyond the local safe head, and only labels blocks present locally.
func (c *Client) stepFinalizedOne(ctx context.Context, targetFinNum uint64) {
	if targetFinNum == 0 {
		return
	}
	// Do not finalize beyond safe or unsafe
	if u := c.eng.UnsafeL2Head(); targetFinNum > u.Number {
		targetFinNum = u.Number
	}
	if s := c.eng.SafeL2Head(); targetFinNum > s.Number {
		targetFinNum = s.Number
	}
	localFin := c.eng.Finalized()
	if localFin.Number >= targetFinNum {
		return
	}
	// Walk back from localFin towards a matching remote ancestor if needed
	anchor := localFin
	// if local finalized is zero, try to anchor at current safe head number
	if anchor.Number == 0 {
		anchor = c.eng.SafeL2Head()
	}
	// Find first height <= anchor that matches remote
	for {
		if rb, ok := c.fetch.BlockByNumber(ctx, anchor.Number); ok {
			if rb.Hash == anchor.Hash {
				break
			}
		} else {
			return
		}
		if anchor.Number == 0 {
			return
		}
		anchor.Number--
		// refresh anchor hash from local if available
		if lr, err := c.l2.L2BlockRefByHash(ctx, anchor.Hash); err == nil {
			anchor = lr
		}
	}
	nextNum := anchor.Number + 1
	if nextNum > targetFinNum {
		return
	}
	// Only set finalized if the next block exists locally (safe loop will ingest it)
	if rb, ok := c.fetch.BlockByNumber(ctx, nextNum); ok {
		if _, err := c.l2.L2BlockRefByHash(ctx, rb.Hash); err == nil {
			c.applyFinalized(ctx, rb)
		}
	}
}

// advanceSafeTo increments local safe towards the target using sequential block-by-number RPCs.
func (c *Client) advanceSafeTo(ctx context.Context, targetSafeNum uint64) {
	local := c.eng.SafeL2Head()
	if local.Number < targetSafeNum {
		c.log.Info("Advancing safe head", "from", local.Number, "to", targetSafeNum)
	}
	for local.Number < targetSafeNum {
		nextNum := local.Number + 1
		b, ok := c.fetch.BlockByNumber(ctx, nextNum)
		if !ok {
			return
		}
		// Ensure block is present locally by committing its payload if missing.
		if _, err := c.l2.L2BlockRefByHash(ctx, b.Hash); err != nil {
			build := c.buildEnvelopeFn
			if build == nil {
				build = c.buildEnvelopeByNumber
			}
			if env, ok := build(ctx, nextNum); ok {
				signed := &opsigner.SignedExecutionPayloadEnvelope{Envelope: env}
				if err := c.eng.CommitBlock(ctx, signed); err != nil {
					c.log.Warn("CommitBlock failed", "num", nextNum, "err", err)
					return
				}
			} else {
				c.log.Warn("Failed to build payload envelope for block", "num", nextNum)
				return
			}
		}
		if b.ParentHash != local.Hash {
			// Walk back from the target head to find a connecting block
			probeNum := targetSafeNum
			connected := false
			for probeNum > local.Number {
				pb, ok := c.fetch.BlockByNumber(ctx, probeNum)
				if !ok {
					return
				}
				if _, err := c.l2.L2BlockRefByHash(ctx, pb.Hash); err != nil {
					build := c.buildEnvelopeFn
					if build == nil {
						build = c.buildEnvelopeByNumber
					}
					if env, ok := build(ctx, probeNum); ok {
						signed := &opsigner.SignedExecutionPayloadEnvelope{Envelope: env}
						if err := c.eng.CommitBlock(ctx, signed); err != nil {
							c.log.Warn("CommitBlock failed (backtrack)", "num", probeNum, "err", err)
							return
						}
					} else {
						c.log.Warn("Failed to build payload envelope (backtrack)", "num", probeNum)
						return
					}
				}
				if pb.ParentHash == local.Hash {
					b = pb
					connected = true
					break
				}
				probeNum--
			}
			if !connected {
				return
			}
		}
		c.log.Info("Applying safe block", "num", b.Number, "hash", b.Hash)
		c.applySafe(ctx, b)
		local = b
	}
}

// stepSafeOne advances safe by at most one block toward targetSafeNum.
// It finds a common ancestor with the remote and commits exactly the next block.
func (c *Client) stepSafeOne(ctx context.Context, targetSafeNum uint64) {
	local := c.eng.SafeL2Head()
	if local.Number >= targetSafeNum {
		return
	}
	// Find common ancestor by comparing hashes at the same height, walking back
	anchor := local
	for {
		// Remote block at current anchor height
		rb, ok := c.fetch.BlockByNumber(ctx, anchor.Number)
		if !ok {
			return
		}
		if rb.Hash == anchor.Hash {
			break
		}
		if anchor.Number == 0 {
			return
		}
		// step back one height locally
		anchor.Number--
		// best-effort refresh of local anchor hash if available
		if lr, err := c.l2.L2BlockRefByHash(ctx, anchor.Hash); err == nil {
			anchor = lr
		}
	}
	// Propose the next block after the matching anchor
	nextNum := anchor.Number + 1
	if nextNum > targetSafeNum {
		return
	}
	nb, ok := c.fetch.BlockByNumber(ctx, nextNum)
	if !ok {
		return
	}
	if nb.ParentHash != anchor.Hash {
		return
	}
	// Compare against local EL canonical block at the same height. If the local canonical
	// does not match the remote block, reorg unsafe back to the local canonical block
	// and try again on the next tick.
	if localAtNext, err := c.l2.L2BlockRefByNumber(ctx, nextNum); err == nil {
		if localAtNext.Hash != nb.Hash {
			c.eng.SetUnsafeHead(localAtNext)
			c.eng.TryUpdateEngine(ctx)
			return
		}
	}
	// Ensure present locally; if not, commit its payload
	if _, err := c.l2.L2BlockRefByHash(ctx, nb.Hash); err != nil {
		build := c.buildEnvelopeFn
		if build == nil {
			build = c.buildEnvelopeByNumber
		}
		if env, ok := build(ctx, nextNum); ok {
			signed := &opsigner.SignedExecutionPayloadEnvelope{Envelope: env}
			if err := c.eng.CommitBlock(ctx, signed); err != nil {
				c.log.Warn("CommitBlock failed (single-step)", "num", nextNum, "err", err)
				return
			}
		} else {
			c.log.Warn("Failed to build payload envelope (single-step)", "num", nextNum)
			return
		}
	}
	c.log.Info("Applying safe block", "num", nb.Number, "hash", nb.Hash)
	c.applySafe(ctx, nb)
}

// buildEnvelopeByNumber reconstructs an ExecutionPayloadEnvelope from JSON-RPC for the given block number.
func (c *Client) buildEnvelopeByNumber(ctx context.Context, num uint64) (*eth.ExecutionPayloadEnvelope, bool) {
	rf, ok := c.fetch.(*rpcFetcher)
	if !ok {
		return nil, false
	}
	hexNum := "0x" + strconv.FormatUint(num, 16)
	var full opsources.RPCBlock
	if err := rf.cli.CallContext(ctx, &full, "eth_getBlockByNumber", hexNum, true); err != nil {
		return nil, false
	}
	env, err := full.ExecutionPayloadEnvelope(false)
	if err != nil {
		return nil, false
	}
	return env, true
}

// parseHexUint64 parses 0x-prefixed hex numbers to uint64
func parseHexUint64(s string) (uint64, bool) {
	if len(s) < 3 || s[:2] != "0x" {
		return 0, false
	}
	var v uint64
	for i := 2; i < len(s); i++ {
		ch := s[i]
		var d byte
		switch {
		case '0' <= ch && ch <= '9':
			d = ch - '0'
		case 'a' <= ch && ch <= 'f':
			d = ch - 'a' + 10
		case 'A' <= ch && ch <= 'F':
			d = ch - 'A' + 10
		default:
			return 0, false
		}
		v = (v << 4) | uint64(d)
	}
	return v, true
}

// BlockFetcher abstracts fetching blocks and head numbers for safe/finalized.
type BlockFetcher interface {
	SafeHeadNumber(ctx context.Context) (uint64, bool)
	FinalizedHeadNumber(ctx context.Context) (uint64, bool)
	BlockByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, bool)
}

type rpcFetcher struct{ cli client.RPC }

func (r *rpcFetcher) SafeHeadNumber(ctx context.Context) (uint64, bool) {
	if b, ok := fetchBlockByTag(ctx, r.cli, "safe"); ok {
		return b.Number, true
	}
	return 0, false
}

func (r *rpcFetcher) FinalizedHeadNumber(ctx context.Context) (uint64, bool) {
	if b, ok := fetchBlockByTag(ctx, r.cli, "finalized"); ok {
		return b.Number, true
	}
	return 0, false
}

func (r *rpcFetcher) BlockByNumber(ctx context.Context, num uint64) (eth.L2BlockRef, bool) {
	hex := "0x" + strconv.FormatUint(num, 16)
	var res struct {
		Hash       common.Hash `json:"hash"`
		Number     string      `json:"number"`
		ParentHash common.Hash `json:"parentHash"`
		Timestamp  string      `json:"timestamp"`
	}
	if err := r.cli.CallContext(ctx, &res, "eth_getBlockByNumber", hex, false); err != nil {
		return eth.L2BlockRef{}, false
	}
	if res.Hash == (common.Hash{}) {
		return eth.L2BlockRef{}, false
	}
	n, ok := parseHexUint64(res.Number)
	if !ok {
		return eth.L2BlockRef{}, false
	}
	t, _ := parseHexUint64(res.Timestamp)
	return eth.L2BlockRef{Hash: res.Hash, Number: n, ParentHash: res.ParentHash, Time: t}, true
}

// computePayloadID deterministically derives a payload ID from parent hash and payload attributes (no Engine API).
func computePayloadID(parent common.Hash, attrs *eth.PayloadAttributes) eth.PayloadID {
	h := sha256.New()
	h.Write(parent[:])
	_ = binary.Write(h, binary.BigEndian, attrs.Timestamp)
	h.Write(attrs.PrevRandao[:])
	h.Write(attrs.SuggestedFeeRecipient[:])
	_ = binary.Write(h, binary.BigEndian, attrs.NoTxPool)
	_ = binary.Write(h, binary.BigEndian, uint64(len(attrs.Transactions)))
	for _, tx := range attrs.Transactions {
		_ = binary.Write(h, binary.BigEndian, uint64(len(tx)))
		h.Write(tx)
	}
	if attrs.GasLimit != nil {
		_ = binary.Write(h, binary.BigEndian, *attrs.GasLimit)
	}
	if attrs.EIP1559Params != nil {
		h.Write(attrs.EIP1559Params[:])
	}
	if attrs.MinBaseFee != nil {
		_ = binary.Write(h, binary.BigEndian, *attrs.MinBaseFee)
	}
	var out eth.PayloadID
	copy(out[:], h.Sum(nil)[:8])
	return out
}
