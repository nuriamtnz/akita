package writeback

import (
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
	"github.com/sarchlab/akita/v4/tracing"
)

type topParser struct {
	cache *Comp
}

func (p *topParser) Tick() bool {
	if p.cache.state != cacheStateRunning {
		return false
	}

	req := p.cache.topPort.PeekIncoming()
	if req == nil {
		return false
	}

	if !p.cache.dirStageBuffer.CanPush() {
		return false
	}

	trans := &transaction{
		id: sim.GetIDGenerator().Generate(),
	}

	//PREFETCHING IMPLEMENTATION NURIA
	//detectar si es prefetch desde el ID del request
	// reqID := req.Meta().ID
	// if strings.HasSuffix(reqID, "_PREFETCH") {
	// 	trans.Prefetch = true
	// }

	switch req := req.(type) {
	case *mem.ReadReq:
		trans.read = req
	case *mem.WriteReq:
		trans.write = req
	}

	p.cache.dirStageBuffer.Push(trans)

	p.cache.inFlightTransactions = append(p.cache.inFlightTransactions, trans)

	tracing.TraceReqReceive(req, p.cache)

	//CONTADORES NURIA
	if readReq, ok := req.(*mem.ReadReq); ok {
		msgId := tracing.MsgIDAtReceiver(readReq, p.cache)
		if readReq.Prefetch {
			tracing.AddTaskStep(msgId, p.cache, "top-prefetch-read-req")
		} else {
			tracing.AddTaskStep(msgId, p.cache, "top-demand-read-req")
		}
	}

	p.cache.topPort.RetrieveIncoming()

	return true
}
