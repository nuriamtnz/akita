package writeback

import (
	"fmt"

	"github.com/sarchlab/akita/v4/mem/cache"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/mem/vm"
	"github.com/sarchlab/akita/v4/pipelining"
	"github.com/sarchlab/akita/v4/sim"
	"github.com/sarchlab/akita/v4/tracing"
)

type dirPipelineItem struct {
	trans *transaction
}

func (i dirPipelineItem) TaskID() string {
	return i.trans.id + "_dir_pipeline"
}

type directoryStage struct {
	cache    *Comp
	pipeline pipelining.Pipeline
	buf      sim.Buffer
}

func (ds *directoryStage) Tick() (madeProgress bool) {
	madeProgress = ds.acceptNewTransaction() || madeProgress

	madeProgress = ds.pipeline.Tick() || madeProgress

	madeProgress = ds.processTransaction() || madeProgress

	return madeProgress
}

func (ds *directoryStage) processTransaction() bool {
	madeProgress := false

	for i := 0; i < ds.cache.numReqPerCycle; i++ {
		item := ds.buf.Peek()
		if item == nil {
			break
		}

		trans := item.(dirPipelineItem).trans

		addr := trans.accessReq().GetAddress()
		cacheLineID, _ := getCacheLineID(addr, ds.cache.log2BlockSize)

		if _, evicting := ds.cache.evictingList[cacheLineID]; evicting {
			break
		}

		if trans.read != nil {
			madeProgress = ds.doRead(trans) || madeProgress
			continue
		}

		madeProgress = ds.doWrite(trans) || madeProgress
	}

	return madeProgress
}

func (ds *directoryStage) acceptNewTransaction() bool {
	madeProgress := false

	for i := 0; i < ds.cache.numReqPerCycle; i++ {
		if !ds.pipeline.CanAccept() {
			break
		}

		item := ds.cache.dirStageBuffer.Peek()
		if item == nil {
			break
		}

		trans := item.(*transaction)
		ds.pipeline.Accept(dirPipelineItem{trans})
		ds.cache.dirStageBuffer.Pop()

		madeProgress = true
	}

	return madeProgress
}

func (ds *directoryStage) Reset() {
	ds.pipeline.Clear()
	ds.buf.Clear()
	ds.cache.dirStageBuffer.Clear()
}

// NURIA PREFETCH IMPLEMENTATION
func (ds *directoryStage) countPrefetchStats(trans *transaction) bool {
	// Solo cuando es prefetch
	if trans.read == nil || !trans.read.Prefetch {
		return false
	}

	if trans.prefetchStatsCounted {
		return trans.prefetchIsRedundant
	}

	trans.prefetchStatsCounted = true

	pid := trans.read.PID
	addr := trans.read.Address
	blockSize := uint64(1 << ds.cache.log2BlockSize)
	cacheLineID := addr / blockSize * blockSize
	msgId := tracing.MsgIDAtReceiver(trans.read, ds.cache)

	if ds.cache.directory.Lookup(pid, cacheLineID) != nil {
		// Línea ya en L2
		tracing.AddTaskStep(
			msgId,
			ds.cache,
			"prefetch-req-hit-l2",
		)
		trans.prefetchIsRedundant = true
		return true
	} else if ds.cache.mshr.Query(pid, cacheLineID) != nil {
		// Línea en MSHR
		tracing.AddTaskStep(
			msgId,
			ds.cache,
			"prefetch-req-hit-mshr",
		)
		trans.prefetchIsRedundant = true
		return true
	} else {
		// Línea ni en L2 ni en MSHR
		tracing.AddTaskStep(
			msgId,
			ds.cache,
			"prefetch-req-miss",
		)
		trans.prefetchIsRedundant = false
		return false
	}
}

// PREFECTH IMPLEMENTATION NURIA
func (ds *directoryStage) removeInflightTransaction(trans *transaction) {
	for i, t := range ds.cache.inFlightTransactions {
		if t == trans {
			ds.cache.inFlightTransactions = append(
				ds.cache.inFlightTransactions[:i],
				ds.cache.inFlightTransactions[i+1:]...,
			)
			return
		}
	}
}

func (ds *directoryStage) doRead(trans *transaction) bool {
	cachelineID, _ := getCacheLineID(
		trans.read.Address, ds.cache.log2BlockSize)

	if ds.countPrefetchStats(trans) {
		// Prefetch redundante: la línea ya está en L2 o llegando.
		// Enviar respuesta ligera a L1 para que decremente su contador
		// de prefetches en vuelo, y después descartar.
		if !ds.cache.topPort.CanSend() {
			return false
		}

		prefetchDone := mem.DataReadyRspBuilder{}.
			WithSrc(ds.cache.topPort.AsRemote()).
			WithDst(trans.read.Src).
			WithRspTo(trans.read.ID).
			WithData(nil).
			Build()
		ds.cache.topPort.Send(prefetchDone)

		ds.buf.Pop()
		ds.removeInflightTransaction(trans)
		tracing.TraceReqComplete(trans.read, ds.cache)
		return true
	}

	mshrEntry := ds.cache.mshr.Query(trans.read.PID, cachelineID)
	if mshrEntry != nil {
		return ds.handleReadMSHRHit(trans, mshrEntry)
	}

	block := ds.cache.directory.Lookup(
		trans.read.PID, cachelineID)
	if block != nil {
		return ds.handleReadHit(trans, block)
	}

	return ds.handleReadMiss(trans)
}

func (ds *directoryStage) handleReadMSHRHit(
	trans *transaction,
	mshrEntry *cache.MSHREntry,
) bool {
	trans.mshrEntry = mshrEntry
	mshrEntry.Requests = append(mshrEntry.Requests, trans)

	ds.buf.Pop()

	tracing.AddTaskStep(
		tracing.MsgIDAtReceiver(trans.read, ds.cache),
		ds.cache,
		"read-mshr-hit",
	)

	// Si esta demanda real llega mientras un prefetch está en vuelo,
	// el MSHR hit fue causado por el prefetcher.
	// El primer Requests es quien creó la entrada del MSHR.
	if !trans.read.Prefetch && len(mshrEntry.Requests) > 0 {
		if firstTrans, ok := mshrEntry.Requests[0].(*transaction); ok {
			if firstTrans.read != nil && firstTrans.read.Prefetch {
				tracing.AddTaskStep(
					tracing.MsgIDAtReceiver(trans.read, ds.cache),
					ds.cache,
					"read-mshr-hit-by-prefetch",
				)
			}
		}
	}

	return true
}

func (ds *directoryStage) handleReadHit(
	trans *transaction,
	block *cache.Block,
) bool {
	if block.IsLocked {
		return false
	}

	ok := ds.readFromBank(trans, block)
	if ok {
		tracing.AddTaskStep(
			tracing.MsgIDAtReceiver(trans.read, ds.cache),
			ds.cache,
			"read-hit",
		)
	}

	// log.Printf("%.10f, %s, dir read hit， %s, %04X, %04X, (%d, %d), %v\n",
	// 	now, ds.cache.Name(),
	// 	trans.read.ID,
	// 	trans.read.Address,
	// 	(trans.read.GetAddress()>>ds.cache.log2BlockSize)
	// 	<<ds.cache.log2BlockSize,
	// 	block.SetID, block.WayID,
	// 	nil,
	// )
	return ok
}

func (ds *directoryStage) handleReadMiss(trans *transaction) bool {
	req := trans.read
	cacheLineID, _ := getCacheLineID(req.Address, ds.cache.log2BlockSize)

	if ds.cache.mshr.IsFull() {
		return false
	}

	victim := ds.cache.directory.FindVictim(cacheLineID)
	if victim.IsLocked || victim.ReadCount > 0 {
		return false
	}

	// log.Printf("%.10f, %s, dir read miss， %s, %04X, %04X, (%d, %d), %v\n",
	// 	now, ds.cache.Name(),
	// 	trans.read.ID,
	// 	trans.read.Address,
	// 	(trans.read.GetAddress()>>ds.cache.log2BlockSize)<<
	// 	ds.cache.log2BlockSize,
	// 	victim.SetID, victim.WayID,
	// 	nil,
	// )

	// if trans.read.Prefetch {
	// 	fmt.Printf("Es un prefetch")
	// }

	if ds.needEviction(victim, trans) {
		ok := ds.evict(trans, victim)
		if ok && !trans.read.Prefetch {
			tracing.AddTaskStep(
				tracing.MsgIDAtReceiver(trans.read, ds.cache),
				ds.cache,
				"read-miss",
			)
		}

		return ok
	}

	ok := ds.fetch(trans, victim)
	if ok && !trans.read.Prefetch {
		tracing.AddTaskStep(
			tracing.MsgIDAtReceiver(trans.read, ds.cache),
			ds.cache,
			"read-miss",
		)
	}

	return ok
}

func (ds *directoryStage) doWrite(trans *transaction) bool {
	write := trans.write
	cachelineID, _ := getCacheLineID(write.Address, ds.cache.log2BlockSize)

	mshrEntry := ds.cache.mshr.Query(write.PID, cachelineID)
	if mshrEntry != nil {
		ok := ds.doWriteMSHRHit(trans, mshrEntry)
		tracing.AddTaskStep(
			tracing.MsgIDAtReceiver(trans.write, ds.cache),
			ds.cache,
			"write-mshr-hit",
		)

		return ok
	}

	block := ds.cache.directory.Lookup(trans.write.PID, cachelineID)
	if block != nil {
		ok := ds.doWriteHit(trans, block)
		if ok {
			tracing.AddTaskStep(
				tracing.MsgIDAtReceiver(trans.write, ds.cache),
				ds.cache,
				"write-hit",
			)
		}

		return ok
	}

	ok := ds.doWriteMiss(trans)
	if ok {
		tracing.AddTaskStep(
			tracing.MsgIDAtReceiver(trans.write, ds.cache),
			ds.cache,
			"write-miss",
		)
	}

	return ok
}

func (ds *directoryStage) doWriteMSHRHit(
	trans *transaction,
	mshrEntry *cache.MSHREntry,
) bool {
	trans.mshrEntry = mshrEntry
	mshrEntry.Requests = append(mshrEntry.Requests, trans)

	ds.buf.Pop()

	return true
}

func (ds *directoryStage) doWriteHit(
	trans *transaction,
	block *cache.Block,
) bool {
	if block.IsLocked || block.ReadCount > 0 {
		return false
	}

	return ds.writeToBank(trans, block)
}

func (ds *directoryStage) doWriteMiss(trans *transaction) bool {
	write := trans.write

	if ds.isWritingFullLine(write) {
		return ds.writeFullLineMiss(trans)
	}

	return ds.writePartialLineMiss(trans)
}

func (ds *directoryStage) writeFullLineMiss(trans *transaction) bool {
	write := trans.write
	cachelineID, _ := getCacheLineID(write.Address, ds.cache.log2BlockSize)

	victim := ds.cache.directory.FindVictim(cachelineID)
	if victim.IsLocked || victim.ReadCount > 0 {
		return false
	}

	if ds.needEviction(victim, trans) {
		return ds.evict(trans, victim)
	}

	return ds.writeToBank(trans, victim)
}

func (ds *directoryStage) writePartialLineMiss(trans *transaction) bool {
	write := trans.write
	cachelineID, _ := getCacheLineID(write.Address, ds.cache.log2BlockSize)

	if ds.cache.mshr.IsFull() {
		return false
	}

	victim := ds.cache.directory.FindVictim(cachelineID)
	if victim.IsLocked || victim.ReadCount > 0 {
		return false
	}

	// log.Printf("%.10f, %s, write partial line ，"+
	// " %s, %04X, %04X, (%d, %d), %v\n",
	// 	now, ds.cache.Name(),
	// 	trans.write.ID,
	// 	trans.write.Address, cachelineID,
	// 	victim.SetID, victim.WayID,
	// 	write.Data,
	// )

	if ds.needEviction(victim, trans) {
		return ds.evict(trans, victim)
	}

	return ds.fetch(trans, victim)
}

func (ds *directoryStage) readFromBank(
	trans *transaction,
	block *cache.Block,
) bool {
	numBanks := len(ds.cache.dirToBankBuffers)
	bank := bankID(block, ds.cache.directory.WayAssociativity(), numBanks)
	bankBuf := ds.cache.dirToBankBuffers[bank]

	if !bankBuf.CanPush() {
		return false
	}

	ds.cache.directory.Visit(block)

	block.ReadCount++
	trans.block = block
	trans.action = bankReadHit

	ds.buf.Pop()
	bankBuf.Push(trans)

	return true
}

func (ds *directoryStage) writeToBank(
	trans *transaction,
	block *cache.Block,
) bool {
	numBanks := len(ds.cache.dirToBankBuffers)
	bank := bankID(block, ds.cache.directory.WayAssociativity(), numBanks)
	bankBuf := ds.cache.dirToBankBuffers[bank]

	if !bankBuf.CanPush() {
		return false
	}

	addr := trans.write.Address
	cachelineID, _ := getCacheLineID(addr, ds.cache.log2BlockSize)

	ds.cache.directory.Visit(block)
	block.IsLocked = true
	block.Tag = cachelineID
	block.IsValid = true
	block.PID = trans.write.PID
	trans.block = block
	trans.action = bankWriteHit

	ds.buf.Pop()
	bankBuf.Push(trans)

	return true
}

func (ds *directoryStage) evict(
	trans *transaction,
	victim *cache.Block,
) bool {
	bankNum := bankID(victim,
		ds.cache.directory.WayAssociativity(), len(ds.cache.dirToBankBuffers))
	bankBuf := ds.cache.dirToBankBuffers[bankNum]

	if !bankBuf.CanPush() {
		return false
	}

	var (
		addr uint64
		pid  vm.PID
	)

	if trans.read != nil {
		addr = trans.read.Address
		pid = trans.read.PID
	} else {
		addr = trans.write.Address
		pid = trans.write.PID
	}

	cacheLineID, _ := getCacheLineID(addr, ds.cache.log2BlockSize)

	ds.updateTransForEviction(trans, victim, pid, cacheLineID)
	ds.updateVictimBlockMetaData(victim, cacheLineID, pid)

	ds.buf.Pop()
	bankBuf.Push(trans)

	ds.cache.evictingList[trans.victim.Tag] = true

	// log.Printf("%.10f, %s, directory evict ， %s, %04X, %04X, (%d, %d), %v\n",
	// 	now, ds.cache.Name(),
	// 	trans.accessReq().Meta().ID,
	// 	trans.accessReq().GetAddress(), trans.victim.Tag,
	// 	victim.SetID, victim.WayID,
	// 	nil,
	// )

	return true
}

func (ds *directoryStage) updateVictimBlockMetaData(
	victim *cache.Block,
	cacheLineID uint64,
	pid vm.PID,
) {
	victim.Tag = cacheLineID
	victim.PID = pid
	victim.IsLocked = true
	victim.IsDirty = false
	ds.cache.directory.Visit(victim)
}

func (ds *directoryStage) updateTransForEviction(
	trans *transaction,
	victim *cache.Block,
	pid vm.PID,
	cacheLineID uint64,
) {
	trans.action = bankEvictAndFetch
	trans.victim = &cache.Block{
		PID:          victim.PID,
		Tag:          victim.Tag,
		CacheAddress: victim.CacheAddress,
		DirtyMask:    victim.DirtyMask,
	}
	trans.block = victim
	trans.evictingPID = trans.victim.PID
	trans.evictingAddr = trans.victim.Tag
	trans.evictingDirtyMask = victim.DirtyMask

	if ds.evictionNeedFetch(trans) {
		mshrEntry := ds.cache.mshr.Add(pid, cacheLineID)
		mshrEntry.Block = victim
		mshrEntry.Requests = append(mshrEntry.Requests, trans)
		trans.mshrEntry = mshrEntry
		trans.fetchPID = pid
		trans.fetchAddress = cacheLineID
		trans.action = bankEvictAndFetch
	} else {
		trans.action = bankEvictAndWrite
	}
}

func (ds *directoryStage) evictionNeedFetch(t *transaction) bool {
	if t.write == nil {
		return true
	}

	if ds.isWritingFullLine(t.write) {
		return false
	}

	return true
}

func (ds *directoryStage) fetch(
	trans *transaction,
	block *cache.Block,
) bool {
	var (
		addr uint64
		pid  vm.PID
		req  mem.AccessReq
	)

	if trans.read != nil {
		req = trans.read
		addr = trans.read.Address
		pid = trans.read.PID
	} else {
		req = trans.write
		addr = trans.write.Address
		pid = trans.write.PID
	}

	cacheLineID, _ := getCacheLineID(addr, ds.cache.log2BlockSize)

	bankNum := bankID(block,
		ds.cache.directory.WayAssociativity(), len(ds.cache.dirToBankBuffers))
	bankBuf := ds.cache.dirToBankBuffers[bankNum]

	if !bankBuf.CanPush() {
		return false
	}

	mshrEntry := ds.cache.mshr.Add(pid, cacheLineID)
	trans.mshrEntry = mshrEntry
	trans.block = block
	block.IsLocked = true
	block.Tag = cacheLineID
	block.PID = pid
	block.IsValid = true
	ds.cache.directory.Visit(block)

	tracing.AddTaskStep(
		tracing.MsgIDAtReceiver(req, ds.cache),
		ds.cache,
		fmt.Sprintf("add-mshr-entry-0x%x-0x%x", mshrEntry.Address, block.Tag),
	)

	ds.buf.Pop()

	trans.action = writeBufferFetch
	trans.fetchPID = pid
	trans.fetchAddress = cacheLineID
	bankBuf.Push(trans)

	mshrEntry.Block = block
	mshrEntry.Requests = append(mshrEntry.Requests, trans)

	return true
}

func (ds *directoryStage) isWritingFullLine(write *mem.WriteReq) bool {
	if len(write.Data) != (1 << ds.cache.log2BlockSize) {
		return false
	}

	if write.DirtyMask != nil {
		for _, dirty := range write.DirtyMask {
			if !dirty {
				return false
			}
		}
	}

	return true
}

func (ds *directoryStage) needEviction(victim *cache.Block, trans *transaction) bool {
	if victim.IsPrefetched {
		if trans.read != nil {
			msgId := tracing.MsgIDAtReceiver(trans.read, ds.cache)
			tracing.AddTaskStep(msgId, ds.cache, "prefetch-evict")
		} else if trans.write != nil {
			msgId := tracing.MsgIDAtReceiver(trans.write, ds.cache)
			tracing.AddTaskStep(msgId, ds.cache, "prefetch-evict")
		}
		victim.IsPrefetched = false
		victim.IsPrefetchedFirstUse = false
	}
	return victim.IsValid && victim.IsDirty
}
