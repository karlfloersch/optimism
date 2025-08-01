# 🛡️ DEFENSIVE ENGINE EVENT CONSUMER MAPPING

> **CRITICAL FOR DEFENSIVE IMPLEMENTATION**: Every engine event consumer MUST be handled or the system will fail-fast!

## 📊 COMPREHENSIVE EXTERNAL CONSUMER ANALYSIS

### **🔥 HIGH-IMPACT ENGINE EVENTS (Must Handle Defensively):**

#### **1. TryUpdateEngineEvent (795x frequency!)**
**Producers:**
- `op-node/rollup/driver/state.go:401` - Driver state management
- `op-node/rollup/engine/events.go:397,428,484,497` - Engine internal
- `op-node/rollup/engine/payload_success.go:43,60` - Payload processing
- `op-e2e/actions/helpers/l2_sequencer.go:127` - Test helper

**External Consumers:** ❌ **NONE FOUND** - Only consumed by EngDeriver internally!
**✅ SAFE TO REPLACE** - No external dependencies!

#### **2. ProcessUnsafePayloadEvent**
**Producers:**
- `op-node/rollup/clsync/clsync.go:126` - CLSync payload forwarding

**External Consumers:** ❌ **NONE FOUND** - Only consumed by EngDeriver internally!
**✅ SAFE TO REPLACE** - No external dependencies!

#### **3. PendingSafeUpdateEvent (405x frequency)**
**Producers:**
- `op-node/rollup/engine/events.go:436,446` - Engine internal

**External Consumers:** 🚨 **MULTIPLE CRITICAL DEPENDENCIES**
- `op-node/rollup/attributes/attributes.go:60` - AttributesHandler.onPendingSafeUpdate()
- `op-node/rollup/status/status.go:73` - StatusTracker state tracking
- `op-program/client/driver/program.go:45` - Fault proof program
- `op-e2e/actions/helpers/l2_verifier.go:414` - Test verifier

**⚠️ REQUIRES DEFENSIVE ADAPTERS!**

#### **4. ForkchoiceUpdateEvent**
**External Consumers:** ✅ **ALREADY HANDLED** by our ForkchoiceController!
- `op-node/rollup/finality/finalizer.go:152`
- `op-node/rollup/clsync/clsync.go:78`
- `op-node/rollup/status/status.go:65`
- `op-node/rollup/sequencing/origin_selector.go:58`
- `op-node/rollup/sequencing/sequencer.go:193`

### **⚡ MEDIUM-IMPACT ENGINE EVENTS:**

#### **5. UnsafeUpdateEvent**
**External Consumers:**
- `op-node/rollup/interop/indexing/system.go:176` - Interop system indexing

**⚠️ REQUIRES INTEROP ADAPTER!**

#### **6. CrossUnsafeUpdateEvent**
**External Consumers:**
- `op-node/rollup/status/status.go:76` - StatusTracker state tracking

#### **7. LocalSafeUpdateEvent**
**External Consumers:**
- `op-node/rollup/status/status.go:80` - StatusTracker state tracking
- `op-node/rollup/interop/indexing/system.go:188` - Interop system indexing
- `op-program/client/driver/program.go:72` - Fault proof program

#### **8. CrossSafeUpdateEvent**
**External Consumers:**
- `op-node/rollup/status/status.go:83` - StatusTracker state tracking

#### **9. SafeDerivedEvent**
**External Consumers:**
- `op-node/rollup/finality/finalizer.go:144` - Finalizer safe head tracking
- `op-node/rollup/driver/state.go:270` - Driver state management

### **🔧 SPECIALIZED ENGINE EVENTS:**

#### **10. EngineResetConfirmedEvent**
**External Consumers:**
- `op-node/rollup/status/status.go:124` - StatusTracker reset handling
- `op-node/rollup/sequencing/sequencer.go:191` - Sequencer reset handling
- `op-node/rollup/driver/state.go:260` - Driver reset handling
- `op-program/client/driver/program.go:40` - Fault proof program

#### **11. Build/Payload Events (Sequencer-Specific)**
**External Consumers:**
- `op-node/rollup/sequencing/sequencer.go:171-183` - Multiple build events
- `op-node/rollup/sequencing/sequencer_chaos_test.go:59,115,122,191,194,200` - Test scenarios

## 🛡️ DEFENSIVE IMPLEMENTATION REQUIREMENTS

### **❌ FAIL-FAST RULE: ALL CONSUMERS MUST BE HANDLED**

```go
// 🚨 DEFENSIVE: Every external consumer MUST have an adapter or explicit handling
func (e *EngineStateManager) validateAllConsumersHandled() {
    requiredAdapters := []string{
        "AttributesHandlerAdapter",      // PendingSafeUpdateEvent
        "StatusTrackerAdapter",          // Multiple events
        "FinalizerAdapter",             // SafeDerivedEvent  
        "InteropSystemAdapter",         // UnsafeUpdateEvent, LocalSafeUpdateEvent
        "ProgramDriverAdapter",         // Multiple events (fault proofs)
        "SequencerAdapter",             // Build/payload events
        "DriverStateAdapter",           // Reset, derived events
    }
    
    for _, adapter := range requiredAdapters {
        if !e.hasAdapter(adapter) {
            panic(fmt.Sprintf("DEFENSIVE: Missing required adapter: %s", adapter))
        }
    }
}
```

### **🎯 ADAPTER REQUIREMENTS:**

#### **MANDATORY ADAPTERS:**
1. **StatusTrackerEngineAdapter** - Handles 6+ different engine events
2. **AttributesHandlerEngineAdapter** - PendingSafeUpdateEvent
3. **FinalizerEngineAdapter** - SafeDerivedEvent  
4. **InteropSystemEngineAdapter** - UnsafeUpdateEvent, LocalSafeUpdateEvent
5. **ProgramDriverEngineAdapter** - Multiple events (fault proof compatibility)
6. **SequencerEngineAdapter** - Build/payload events
7. **DriverStateEngineAdapter** - Reset and derived events

#### **DEFENSIVE ERROR HANDLING:**
```go
type EngineEventError struct {
    EventType    string
    ConsumerType string
    Error        error
}

func (e *EngineStateManager) handleEventOrPanic(ctx context.Context, eventType string, handler func() error) {
    if err := handler(); err != nil {
        if e.strictMode {
            panic(fmt.Sprintf("DEFENSIVE: Engine event %s failed: %v", eventType, err))
        }
        e.log.Error("Engine event handler failed", "event", eventType, "error", err)
    }
}
```

## 🚨 CRITICAL DISCOVERY: MOST ENGINE EVENTS ARE INTERNAL!

**KEY INSIGHT:** Most high-frequency engine events (TryUpdateEngine, ProcessUnsafePayload) have **NO external consumers**!

### **✅ SAFE TO REPLACE (No External Dependencies):**
- TryUpdateEngineEvent (795x) - Only EngDeriver internal
- ProcessUnsafePayloadEvent - Only EngDeriver internal  
- PromoteUnsafeEvent - Only EngDeriver internal
- PromotePendingSafeEvent - Only EngDeriver internal
- PromoteLocalSafeEvent - Only EngDeriver internal
- PromoteSafeEvent - Only EngDeriver internal
- PromoteFinalizedEvent - Only EngDeriver internal

### **⚠️ REQUIRES ADAPTERS (External Dependencies):**
- PendingSafeUpdateEvent (405x) - 4 external consumers
- UnsafeUpdateEvent - 1 external consumer (interop)
- LocalSafeUpdateEvent - 3 external consumers
- CrossSafeUpdateEvent - 1 external consumer
- SafeDerivedEvent - 2 external consumers
- EngineResetConfirmedEvent - 4 external consumers

## 🚀 IMPLEMENTATION STRATEGY

### **Phase 1: Internal Engine Events (SAFE)**
Replace all internal-only events with EngineStateManager methods:
- ✅ TryUpdateEngine() - 795x frequency gain
- ✅ ProcessUnsafePayload() 
- ✅ PromoteToUnsafe()
- ✅ PromoteToPendingSafe()
- ✅ PromoteToLocalSafe()
- ✅ PromoteToSafe()
- ✅ PromoteToFinalized()

### **Phase 2: External Events with Adapters (DEFENSIVE)**
Create adapters for all external consumers:
- ⚠️ StatusTracker (6 events)
- ⚠️ AttributesHandler (1 event)
- ⚠️ Finalizer (1 event)
- ⚠️ InteropSystem (2 events)
- ⚠️ ProgramDriver (3 events)
- ⚠️ Sequencer (6+ events)

### **Phase 3: Validation (FAIL-FAST)**
- 🛡️ Defensive validation that all consumers are handled
- 🚨 Panic on any unhandled event/consumer
- ✅ Comprehensive devstack testing

**This approach maximizes safety while delivering massive architectural benefits!** 🎯