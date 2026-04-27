package writearound

import (
	"github.com/sarchlab/akita/v4/mem/cache"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

// PREFETCH IMPLEMETATION NURIA
type PrefetchMode int // patron de go para crear enumerados
const (               // conjunto de constantes de tipo PrefetchMode
	PrefNone         PrefetchMode = iota //0
	PrefNextLine                         //1
	PrefTwoNextLines                     //2
	PrefFarJump                          //3
	PrefLoop                             //4
)

// Comp is a customized L1 cache the for R9nano GPUs.
type Comp struct {
	*sim.TickingComponent
	sim.MiddlewareHolder

	topPort     sim.Port
	bottomPort  sim.Port
	controlPort sim.Port

	numReqPerCycle      int
	log2BlockSize       uint64
	storage             *mem.Storage
	directory           cache.Directory
	mshr                cache.MSHR
	bankLatency         int
	wayAssociativity    int
	addressToPortMapper mem.AddressToPortMapper

	dirBuf   sim.Buffer
	bankBufs []sim.Buffer

	coalesceStage    *coalescer
	directoryStage   *directory
	bankStages       []*bankStage
	parseBottomStage *bottomParser
	respondStage     *respondStage
	controlStage     *controlStage

	maxNumConcurrentTrans    int
	transactions             []*transaction
	postCoalesceTransactions []*transaction

	isPaused bool

	// PREFETCH IMPLEMETATION NURIA
	prefetchMode         PrefetchMode
	prefetchStrideBlocks uint64 //salto lejano en bloques
	prefetchNumLines     uint64 // cuántas líneas prefetchear (para loop)
	totalByteSize        uint64

	// Límite de prefetches en vuelo por caché L1
	inFlightPrefetches    int
	maxInFlightPrefetches int
}

// SetAddressToPortMapper sets the finder that tells which remote port can serve
// the data on a certain address.
func (c *Comp) SetAddressToPortMapper(lmf mem.AddressToPortMapper) {
	c.addressToPortMapper = lmf
}

func (c *Comp) Tick() bool {
	return c.MiddlewareHolder.Tick()
}

type middleware struct {
	*Comp
}

// Tick update the state of the cache
func (m *middleware) Tick() bool {
	madeProgress := false

	if !m.isPaused {
		madeProgress = m.runPipeline() || madeProgress
	}

	madeProgress = m.controlStage.Tick() || madeProgress

	return madeProgress
}

func (m *middleware) runPipeline() bool {
	madeProgress := false
	madeProgress = m.tickRespondStage() || madeProgress
	madeProgress = m.tickParseBottomStage() || madeProgress
	madeProgress = m.tickBankStage() || madeProgress
	madeProgress = m.tickDirectoryStage() || madeProgress
	madeProgress = m.tickCoalesceState() || madeProgress

	return madeProgress
}

func (m *middleware) tickRespondStage() bool {
	madeProgress := false
	for i := 0; i < m.numReqPerCycle; i++ {
		madeProgress = m.respondStage.Tick() || madeProgress
	}

	return madeProgress
}

func (m *middleware) tickParseBottomStage() bool {
	madeProgress := false

	for i := 0; i < m.numReqPerCycle; i++ {
		madeProgress = m.parseBottomStage.Tick() || madeProgress
	}

	return madeProgress
}

func (m *middleware) tickBankStage() bool {
	madeProgress := false
	for _, bs := range m.bankStages {
		madeProgress = bs.Tick() || madeProgress
	}

	return madeProgress
}

func (m *middleware) tickDirectoryStage() bool {
	return m.directoryStage.Tick()
}

func (m *middleware) tickCoalesceState() bool {
	madeProgress := false
	for i := 0; i < m.numReqPerCycle; i++ {
		madeProgress = m.coalesceStage.Tick() || madeProgress
	}

	return madeProgress
}
