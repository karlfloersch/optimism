# Event System Flow Tracer

This package builds a tracer that generates AST-like data structures from op-node's event system to enable confident refactoring.

## 🚀 Quick Start

```bash
# Run basic tests
make test-flow

# Check implementation status
make status

# Start Phase 1 development
make phase1
```

## 📁 Package Structure

```
op-node/flow/
├── FLOW_TRACER_PLAN.md     # Complete project plan and strategy
├── README.md               # This file
├── Makefile               # Development targets
├── tracer.go              # ✅ Main flow tracing implementation
├── ast.go                 # ✅ AST data structures
├── tracer_test.go         # ✅ Unit tests for flow tracer
└── cmd/                   # 🚧 Analysis tools (Phase 3)
```

## 🎯 Current Status

**Phase 1 (✅ COMPLETE)**: Basic event capture infrastructure
- ✅ Flow tracer that implements `event.Tracer` interface
- ✅ Event capture with timing and correlation tracking
- ✅ AST data structures for flow representation
- ✅ Unit test framework for validation
- ✅ **Environment variable integration**: `OP_NODE_FLOW_TRACING=true`
- ✅ **Devstack integration working**: Successfully captures events during real interop tests
- 🚧 **Next**: Add file output and analysis tools (Phase 2)

## 📖 Usage

### Environment Variable Integration (Recommended)

```bash
# Enable flow tracing on any devstack test
OP_NODE_FLOW_TRACING=true go test ./example -run TestExample1 -v

# The op-node will automatically:
# 1. Create and add flow tracer to event system
# 2. Log when flow tracing is enabled
# 3. Capture all events during test execution
```

### Direct API Usage

```go
// Create and add tracer to event system
tracer := flow.NewFlowTracer()
sys.AddTracer(tracer)

// After test execution
stats := tracer.GetStats()
events := tracer.GetEvents()
correlations := tracer.GetCorrelations()
```

### Expected Output

```go
// Tracer captures comprehensive event data
type CapturedEvent struct {
    EmitContext  uint64        // Unique event ID
    DerivContext uint64        // Processing context
    EventName    string        // Event type
    EmitTime     time.Time     // When emitted
    ProcessTime  time.Time     // When processed
    Duration     time.Duration // Processing time
    DeriverName  string        // Which component processed it
    Effect       bool          // Whether it had an effect
}
```

## 🧪 Testing Strategy

### Devstack Integration (Phase 1)

The flow tracer is **imported and used by** devstack tests (not the other way around):

**Architecture:**
- `op-node/flow/` → Flow tracer implementation and unit tests
- `op-devstack/` → Integration tests that import and use the flow tracer
- Real integration happens by adding `tracer := flow.NewFlowTracer()` to devstack tests

**Example Integration:**
```go
// In op-devstack/example/flow_test.go
import "github.com/ethereum-optimism/optimism/op-node/flow"

func TestFlowTracing(gt *testing.T) {
    t := devtest.ParallelT(gt)
    sys := presets.NewSimpleInterop(t)

    // Add flow tracer to op-node's event system
    tracer := flow.NewFlowTracer()
    sys.OpNodeA.AddTracer(tracer)

    // Run test scenario (exercises real flows)
    sys.Supervisor.VerifySyncStatus(dsl.WithAllLocalUnsafeHeadsAdvancedBy(10))

    // Analyze captured flows
    events := tracer.GetEvents()
    // Generate reports...
}
```

### Success Metrics

- **Event Capture Rate**: >95% of events captured
- **Flow Coverage**: All major event types have flow paths
- **Zero Missing Correlations**: All emissions traced to triggers
- **Performance**: <10% overhead in shadow mode

## 🔄 Development Workflow

### Fast Feedback Loop (Target: <5 minutes)

```bash
# 1. Make changes to tracer
vim tracer.go

# 2. Run development loop
make dev-loop

# 3. Check results
cat flow-completeness-report.json
```

### Phase Development

```bash
make phase1  # Basic infrastructure
make phase2  # AST generation
make phase3  # Completeness validation
make phase4  # Pattern analysis
```

## 📊 Expected Outputs

### Phase 1: Event Capture
- `trace-events.json` - All captured events with timing
- Basic completeness statistics

### Phase 2: AST Generation
- `flow-ast-graph.json` - Generated flow graphs
- `flow-patterns-analysis.json` - Identified patterns

### Phase 3: Validation
- `flow-completeness-report.json` - Detailed metrics
- Missing correlation analysis

### Phase 4: Analysis
- `flow-migration-recommendations.json` - Refactoring guidance
- Risk assessment and migration planning

## 🎯 Goals

1. **Capture Complete Event Flows** - Trace all op-node events with high fidelity
2. **Generate AST Representations** - Build analyzable flow graphs
3. **Enable Confident Refactoring** - Provide data-driven migration insights
4. **Fast Feedback Loop** - Test and validate quickly using devstack

## 📚 Key Documents

- **[FLOW_TRACER_PLAN.md](./FLOW_TRACER_PLAN.md)** - Complete project strategy
- **[op-devstack README](../../op-devstack/README.md)** - Integration test framework

## 🛠️ Next Steps

1. **Integrate with devstack** - Modify devstack tests to include flow tracer
2. **Validate event capture** - Ensure >95% capture rate
3. **Build AST generation** - Implement `BuildAST()` algorithm
4. **Add completeness metrics** - Automated validation and reporting

---

*This tracer transforms implicit event control flows into explicit, analyzable representations for confident event system refactoring.*
