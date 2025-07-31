# Event System Flow Tracer & AST Builder

## 🎯 **Project Overview**

Build a tracer that generates AST-like data structures from op-node's event system to enable confident refactoring. The system will learn implicit event control flows and convert them into explicit, analyzable representations.

## 📋 **Core Goals**

1. **Capture Complete Event Control Flows** - Trace all events in op-node with high completeness
2. **Generate AST Representations** - Build structured flow graphs from observed event patterns
3. **Enable Confident Refactoring** - Provide data-driven insights for event system migration
4. **Fast Feedback Loop** - Test quickly using devstack integration tests

## 🧪 **Testing Strategy & Success Metrics**

### **Primary Test Harness: Devstack Integration Tests**

**Why Devstack?**
- Exercises complete interop system with realistic event flows
- Includes error handling, reset flows, sync operations, interop messaging
- Existing infrastructure with fast execution (minutes, not hours)
- Comprehensive coverage of op-node event patterns

**Key Test Scenarios:**
- `op-devstack/example/example_test.go` - Basic interop flows
- `op-acceptance-tests/tests/interop/message/` - Cross-chain messaging flows
- `op-acceptance-tests/tests/interop/sync/` - Synchronization and recovery flows
- `op-devstack/sysgo/system_test.go` - System-level integration

### **Success Metrics for "Working Tracer"**

**Completeness Indicators:**
- ✅ **Event Capture Rate**: >95% of events fired during devstack tests are captured
- ✅ **Flow Coverage**: All major event types have associated flow paths in AST
- ✅ **Node/Edge Density**: AST contains substantial nodes (>50) and edges (>100) for typical test run
- ✅ **Zero Missing Correlations**: All event emissions can be traced to their triggering events
- ✅ **Pattern Recognition**: Identify distinct flow patterns (target: 10-20 unique patterns)

**Performance Indicators:**
- ✅ **Shadow Mode**: Zero performance impact on devstack execution time
- ✅ **Memory Usage**: <50MB additional memory during typical test run
- ✅ **Processing Speed**: AST generation completes within 30s of test completion

**Quality Indicators:**
- ✅ **State Correlation**: Successfully correlates state changes with event transitions
- ✅ **Error Flow Capture**: Captures error handling and recovery patterns
- ✅ **Timing Accuracy**: Event sequence timing matches observed execution order

## 🔄 **Fast Feedback Loop Design**

### **Development Cycle (Target: <5 minutes)**

```bash
# 1. Run subset of devstack tests with tracer
make test-devstack-subset

# 2. Generate AST and analyze completeness
go run ./cmd/flow-analyzer --input=trace.json --validate

# 3. View results and identify gaps
cat flow-analysis-report.json | jq '.completeness_metrics'

# 4. Fix tracer bugs based on gaps
# 5. Repeat
```

### **Validation Outputs**

**Trace Completeness Report:**
```json
{
  "event_capture_rate": 0.97,
  "total_events_observed": 1247,
  "total_events_traced": 1210,
  "missing_correlations": [],
  "flow_patterns_identified": 15,
  "ast_metrics": {
    "nodes": 67,
    "edges": 134,
    "max_depth": 8,
    "cycles_detected": 3
  }
}
```

**Flow Pattern Summary:**
```json
{
  "patterns": [
    {
      "name": "L1-Head-Update-Flow",
      "frequency": 45,
      "avg_duration": "125ms",
      "success_rate": 1.0
    },
    {
      "name": "Engine-Reset-Recovery",
      "frequency": 3,
      "avg_duration": "2.1s",
      "success_rate": 0.67,
      "error_patterns": ["timeout", "state-mismatch"]
    }
  ]
}
```

## 🏗️ **Implementation Plan**

### **Phase 1: Foundation (Week 1)**
- [ ] Create flow tracer package structure in `op-service/event/flow/`
- [ ] Implement basic event capture infrastructure
- [ ] Add tracer integration to devstack test runner
- [ ] Validate basic event counting and timing

**Deliverable**: Can count events during devstack execution

### **Phase 2: AST Generation (Week 2)**
- [ ] Build flow graph data structures
- [ ] Implement pattern recognition algorithms
- [ ] Add state correlation tracking
- [ ] Create JSON output format for analysis

**Deliverable**: Generates basic AST from captured events

### **Phase 3: Completeness & Validation (Week 3)**
- [ ] Add missing event correlation detection
- [ ] Implement completeness metrics
- [ ] Build validation and analysis tools
- [ ] Add automated report generation

**Deliverable**: Comprehensive completeness reporting

### **Phase 4: Pattern Analysis (Week 4)**
- [ ] Advanced pattern recognition for common flows
- [ ] Error pattern identification
- [ ] Performance bottleneck detection
- [ ] Migration planning recommendations

**Deliverable**: Actionable insights for refactoring

## 🛠️ **Technical Architecture**

### **Core Components**

```
op-node/flow/
├── tracer.go          # Main flow tracing implementation
├── ast.go             # AST data structures and builders
├── patterns.go        # Pattern recognition algorithms
├── validator.go       # Completeness validation
├── analyzer.go        # Flow analysis and reporting
└── devstack_test.go   # Integration tests
```

### **Data Flow**

```
Devstack Tests → Flow Tracer → Event Capture → AST Builder → Analysis Reports
                      ↓              ↓             ↓             ↓
                 Shadow Mode    Raw Events    Flow Graph    Insights JSON
```

### **Integration Points**

**Devstack Integration:**
```go
// Add to devstack test setup
func setupFlowTracer(t devtest.Testing) *flow.Tracer {
    tracer := flow.NewTracer()
    sys.AddTracer(tracer) // Plug into existing event system
    t.Cleanup(tracer.GenerateReport)
    return tracer
}
```

**Output Storage:**
- **Development**: Files in `/tmp/flow-traces/`
- **CI**: Structured logs + artifacts
- **Analysis**: JSON format for tooling integration

### **Expected File Outputs**

**Raw Trace Data:**
- `trace-events.json` - All captured events with timing
- `trace-correlations.json` - Event causation relationships
- `trace-state-changes.json` - System state snapshots

**Analysis Reports:**
- `flow-completeness-report.json` - Validation metrics
- `flow-patterns-analysis.json` - Identified patterns
- `flow-ast-graph.json` - Generated AST structure
- `flow-migration-recommendations.json` - Refactoring insights

## 🚀 **Getting Started**

### **Prerequisites**
- Working devstack environment
- Go 1.21+
- Existing event system understanding

### **Quick Start**
```bash
# 1. Run devstack with flow tracing enabled
cd op-devstack && go test -run TestExample1 -trace-flows

# 2. Analyze results
go run op-node/flow/cmd/analyzer trace-events.json

# 3. View completeness report
cat flow-completeness-report.json
```

### **Key Questions to Validate**

During development, we should be able to answer:

1. **"How complete is our tracing?"** → Check capture rate and missing correlations
2. **"What are the main flow patterns?"** → Review patterns analysis
3. **"Are we missing critical events?"** → Validate against known event types
4. **"Can we reproduce flow behavior?"** → Compare AST predictions to actual execution
5. **"What should we refactor first?"** → Use complexity metrics and frequency data

## 🎯 **Immediate Next Steps**

1. **Create basic tracer infrastructure** that can capture events during devstack tests
2. **Validate event counting** - ensure we're seeing the events we expect
3. **Add correlation tracking** - connect event emissions to their triggers
4. **Generate first AST** - build initial flow graph from simple test case
5. **Measure completeness** - establish baseline metrics

## 🔍 **Success Criteria by Phase**

**Phase 1 Success**: "We can trace devstack events"
- Captures >90% of events without crashing
- Minimal performance overhead (<10% slower)
- Basic timing and correlation data

**Phase 2 Success**: "We can generate ASTs"
- Produces meaningful flow graphs with >50 nodes
- Identifies >10 distinct patterns
- JSON output validates and loads correctly

**Phase 3 Success**: "We can measure completeness"
- Automated completeness metrics show >95% capture rate
- Zero critical missing correlations
- Fast feedback loop operational (<5 min)

**Phase 4 Success**: "We can guide refactoring"
- Clear migration recommendations
- Risk assessment for different flows
- Actionable next steps for event system refactor

---

*This document serves as the blueprint for building confidence in our event system refactoring through data-driven flow analysis and AST generation.*
