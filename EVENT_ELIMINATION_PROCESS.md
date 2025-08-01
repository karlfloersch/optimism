# Event Elimination Process: From Event-Driven to Imperative

This document captures the proven step-by-step process for eliminating events from the op-node codebase, transforming event-driven flows into explicit imperative calls for improved debuggability.

## Overview

Our goal is to replace implicit event emissions and handlers with explicit method calls, making the control flow easier to trace and debug. This process has been successfully applied to eliminate:

- ✅ **TryUpdateEngineEvent** (585 occurrences → 0)
- ✅ **ForkchoiceUpdateEvent** (375+ occurrences → controlled)
- ✅ **ForkchoiceRequestEvent** (multiple sites → 0)

## The Step-by-Step Process

### Phase 1: Analysis & Planning 📊

#### 1.1 Identify Target Event
```bash
# Find all references to the event
grep -r "EventName" --include="*.go" . | grep -v "_test.go"

# Analyze frequency with tracing (if available)
OP_NODE_FLOW_TRACING=true go test ./op-devstack/sysgo -run TestSystem -timeout 3m -v > trace.log
grep -o '"event-name"' trace.log | wc -l
```

#### 1.2 Map Producers and Consumers
- **Producers**: Where is the event emitted? (`emitter.Emit(ctx, EventName{...})`)
- **Consumers**: Where is the event handled? (`case EventName:`)
- **Data Flow**: What data does the event carry? What state changes result?

#### 1.3 Assess Complexity
- **Simple events**: Pure getters, conditional logic, single consumers
- **Complex events**: Multiple consumers, cascading effects, timing dependencies
- **Start with simple events first**

### Phase 2: Design Imperative Interface 🔧

#### 2.1 Create Imperative Method Signature
```go
// Replace this event-driven pattern:
// emitter.Emit(ctx, SomeEvent{Data: data})

// With this imperative method:
func (manager *StateManager) ProcessSomeOperation(ctx context.Context, data DataType) error {
    // Direct method call with same logic as event handler
}
```

#### 2.2 Design Dependency Injection
For cross-package calls, create interfaces:
```go
// Define interface for the capability you need
type SomeOperationRequester interface {
    ProcessSomeOperationImperative(ctx context.Context, data DataType) error
}

// Add to struct that needs to make requests
type Consumer struct {
    operationRequester SomeOperationRequester // Optional, can be nil
}
```

### Phase 3: Implementation 🛠️

#### 3.1 Add Imperative Method to State Manager
```go
// Add to the appropriate state manager (e.g., EngineStateManager)
func (esm *EngineStateManager) ProcessSomeOperation(ctx context.Context, data DataType) error {
    esm.log.Debug("Processing operation imperatively", "data", data)

    // Same logic as original event handler
    // Handle errors defensively
    // Log for debugging

    return nil
}
```

#### 3.2 Add Controller Bridge Method
```go
// Add to controller (e.g., EngineController) for external access
func (e *EngineController) ProcessSomeOperationImperative(ctx context.Context, data DataType) error {
    if e.stateManager != nil {
        return e.stateManager.ProcessSomeOperation(ctx, data)
    }
    // Option A: Fallback for gradual migration
    // Option B: Fail fast for complete elimination (recommended)
    e.log.Error("CRITICAL: stateManager is nil - this should never happen")
    return fmt.Errorf("stateManager not initialized")
}
```

#### 3.3 Update Consumer Constructor
```go
// Update constructors to accept the imperative interface
func NewConsumer(..., operationRequester SomeOperationRequester) *Consumer {
    return &Consumer{
        // ... other fields
        operationRequester: operationRequester,
    }
}
```

#### 3.4 Wire Dependencies in Driver
```go
// In driver.go or equivalent wiring location
consumer := NewConsumer(..., engineController) // Pass controller as interface
```

### Phase 4: Replace Event Handling 🔄

#### 4.1 Replace Consumer Side (Event Handlers)
```go
// Before: Event handler
case SomeEvent:
    // complex logic here
    result := process(x.Data)
    return result

// After: Imperative call
case SomeEvent:
    if err := d.stateManager.ProcessSomeOperation(ctx, x.Data); err != nil {
        d.log.Debug("ProcessSomeOperation completed with error", "error", err)
    }
```

#### 4.2 Replace Producer Side (Event Emissions)
```go
// Before: Event emission
emitter.Emit(ctx, SomeEvent{Data: data})

// After: Imperative call with wrapper method
func (d *Consumer) requestSomeOperation(ctx context.Context, data DataType) {
    if d.operationRequester == nil {
        d.log.Error("CRITICAL: operationRequester is nil - check wiring")
        return
    }
    if err := d.operationRequester.ProcessSomeOperationImperative(ctx, data); err != nil {
        d.log.Debug("Imperative operation completed with error", "error", err)
    }
}

// Replace emission sites
// emitter.Emit(ctx, SomeEvent{Data: data})
d.requestSomeOperation(ctx, data)
```

### Phase 5: Testing & Validation ✅

#### 5.1 Unit Tests
```go
func TestStateManager_ProcessSomeOperation_Success(t *testing.T) {
    // Test the new imperative method
    stateManager := NewStateManager(mockController, logger)

    err := stateManager.ProcessSomeOperation(ctx, testData)

    assert.NoError(t, err)
    // Verify expected state changes
}
```

#### 5.2 Integration Tests
```bash
# Run comprehensive system tests
OP_NODE_FLOW_TRACING=true go test ./op-devstack/sysgo -run TestSystem -timeout 3m -v > validation.log

# Verify event elimination
grep -o '"event-name"' validation.log | wc -l  # Should be 0

# Ensure tests pass
grep "PASS\|FAIL" validation.log
```

#### 5.3 Update Test Expectations
```go
// Update any tests that expect the old events
// Remove emitter.ExpectOnce(OldEvent{}) calls
// Update to test imperative methods directly
```

#### 5.4 Test Fallbacks (When Needed)
If unit tests break because they rely on event-driven behavior that can't be easily mocked imperatively, consider adding a **test-only fallback**:

```go
func (d *Component) requestSomeOperation(ctx context.Context) {
    if d.operationRequester == nil {
        // FALLBACK: This event emission exists solely for unit test compatibility.
        // In production, operationRequester is always non-nil and uses imperative calls.
        // In tests, operationRequester is nil, so we fall back to the old event
        // to maintain existing test expectations and enable proper mocking.
        d.log.Debug("Component operationRequester is nil - falling back to SomeEvent emission for test compatibility")
        d.emitter.Emit(ctx, engine.SomeEvent{})
        return
    }
    // Production imperative call
    if err := d.operationRequester.RequestSomeOperationImperative(ctx, d.emitter); err != nil {
        d.log.Debug("Imperative operation request completed with error", "error", err)
    }
}
```

**When to use fallbacks:**
- ✅ Complex chaos tests or simulations that heavily rely on event-driven behavior
- ✅ Legacy tests where refactoring to imperatives would be disproportionately complex
- ❌ Regular unit tests (prefer updating them to use imperative interfaces)
- ❌ Production code paths (should always use imperative approach)

### Phase 6: Cleanup & Documentation 📚

#### 6.1 Remove Fallbacks (Recommended)
For complete elimination, remove fallback event emissions:
```go
// Instead of:
if d.operationRequester != nil {
    // imperative call
} else {
    // fallback event emission ← REMOVE THIS
}

// Use:
if d.operationRequester == nil {
    d.log.Error("CRITICAL: operationRequester is nil")
    return
}
// imperative call only
```

#### 6.2 Final Validation
```bash
# Verify zero remaining emissions in production code
grep -r "EventName{}" --include="*.go" . | grep -v "_test.go" | wc -l  # Should be 0
```

#### 6.3 Commit with Clear Message
```bash
git commit -m "Eliminate EventName with imperative ProcessSomeOperation method

- Add ProcessSomeOperation method to StateManager
- Replace EventName handler with direct imperative call
- Update all emission sites with imperative calls
- Wire dependencies through constructor injection
- All tests pass with zero EventName emissions

Improves debuggability by making control flow explicit."
```

## Key Success Patterns

### ✅ Start Simple
- Begin with events that have single consumers
- Choose events with pure getter logic
- Avoid events with complex timing dependencies initially

### ✅ Defensive Programming
- Add nil checks with clear error messages
- Log imperative calls for debugging
- Use fail-fast behavior instead of silent fallbacks

### ✅ Incremental Validation
- Test after each phase
- Use tracing to verify elimination
- Maintain passing tests throughout

### ✅ Multiple Function Calls
- Some events trigger multiple effects - replicate with multiple imperative calls
- Example: One event emission → Multiple method calls in imperative version

### ✅ Interface Design
- Use dependency injection for cross-package calls
- Keep interfaces focused and minimal
- Make dependencies explicit in constructors

## Common Pitfalls to Avoid

### ❌ Silent Fallbacks
Don't hide failures with silent fallbacks - fail explicitly:
```go
// BAD
if requester == nil {
    // silently emit old event
}

// GOOD
if requester == nil {
    log.Error("CRITICAL: requester is nil")
    return
}
```

### ❌ Incomplete Wiring
Always wire dependencies in all code paths:
- Production code
- Test code
- E2E test helpers
- Development utilities

### ❌ Ignoring Test Failures
Event elimination can reveal subtle timing/ordering dependencies. If tests fail:
- Investigate the dependency carefully
- Consider if the event has critical timing behavior
- May need to choose a different event or handle the dependency explicitly

## Metrics of Success

- **Event Emissions**: Target event count should reach 0
- **Test Coverage**: All existing tests should continue passing
- **Code Clarity**: Control flow should be more explicit and traceable
- **Debug Experience**: Easier to set breakpoints and trace execution paths

## Example: Complete ForkchoiceRequestEvent Elimination

**Before**:
- Multiple emission sites across sequencer, clsync, e2e helpers
- Event-driven communication between components
- Implicit control flow

**After**:
- `RequestForkchoiceUpdateImperative()` method in `EngineController`
- `EngineForkchoiceRequester` interface for dependency injection
- Direct method calls with explicit error handling
- 0 event emissions in production code
- All tests passing

This process has been validated across multiple events and consistently produces clean, debuggable code while maintaining full behavioral compatibility.
