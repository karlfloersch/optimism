package flow

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// StaticAnalyzer parses Go source code to extract real producer-consumer relationships
type StaticAnalyzer struct {
	fileSet   *token.FileSet
	producers map[string][]ProducerInfo // event_name -> [producer_locations...]
	consumers map[string][]ConsumerInfo // event_name -> [consumer_handlers...]
}

// ProducerInfo represents where an event is emitted
type ProducerInfo struct {
	File     string // "payload_process.go"
	Line     int    // 54
	Function string // "onPayloadProcess"
	Package  string // "engine"
}

// ConsumerInfo represents where an event is handled
type ConsumerInfo struct {
	File         string // "sequencer.go"
	Line         int    // 184
	HandlerFunc  string // "onPayloadSuccess"
	ReceiverType string // "Sequencer"
}

// NewStaticAnalyzer creates a new static code analyzer
func NewStaticAnalyzer() *StaticAnalyzer {
	return &StaticAnalyzer{
		fileSet:   token.NewFileSet(),
		producers: make(map[string][]ProducerInfo),
		consumers: make(map[string][]ConsumerInfo),
	}
}

// AnalyzeDirectory scans all Go files in a directory for event patterns
func (sa *StaticAnalyzer) AnalyzeDirectory(rootDir string) error {
	fmt.Printf("🔍 Analyzing Go files in %s...\n", rootDir)

	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process .go files (skip tests for now)
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		return sa.analyzeFile(path)
	})
}

// analyzeFile parses a single Go file and extracts event patterns
func (sa *StaticAnalyzer) analyzeFile(filename string) error {
	// Parse the Go file into an AST
	src, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", filename, err)
	}

	file, err := parser.ParseFile(sa.fileSet, filename, src, parser.ParseComments)
	if err != nil {
		// Skip files with parse errors (might be different Go version, etc.)
		return nil
	}

	// Walk the AST looking for patterns
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			// Look for emitter.Emit() calls
			sa.checkEmitterCall(node, filename)
		case *ast.TypeSwitchStmt:
			// Look for OnEvent type switches
			sa.checkEventSwitch(node, filename)
		}
		return true
	})

	return nil
}

// checkEmitterCall looks for patterns like: emitter.Emit(ctx, SomeEvent{...})
func (sa *StaticAnalyzer) checkEmitterCall(call *ast.CallExpr, filename string) {
	// Check if this is a method call on something ending with "emitter"
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Emit" {
		// Check if receiver ends with "emitter"
		if sa.isEmitterReceiver(sel.X) {
			// Extract the event type from the second argument
			if len(call.Args) >= 2 {
				eventType := sa.extractEventType(call.Args[1])
				if eventType != "" {
					position := sa.fileSet.Position(call.Pos())
					producer := ProducerInfo{
						File:     filepath.Base(filename),
						Line:     position.Line,
						Function: sa.findEnclosingFunction(call, filename),
						Package:  sa.extractPackageName(filename),
					}

					sa.producers[eventType] = append(sa.producers[eventType], producer)
					fmt.Printf("📤 Found PRODUCER: %s emitted in %s:%d\n", eventType, producer.File, producer.Line)
				}
			}
		}
	}
}

// checkEventSwitch looks for OnEvent methods with type switch statements
func (sa *StaticAnalyzer) checkEventSwitch(switchStmt *ast.TypeSwitchStmt, filename string) {
	// Check if this is inside an OnEvent method
	if !sa.isInOnEventMethod(switchStmt, filename) {
		return
	}

	// Extract event types from case clauses
	for _, stmt := range switchStmt.Body.List {
		if caseClause, ok := stmt.(*ast.CaseClause); ok {
			for _, expr := range caseClause.List {
				eventType := sa.extractTypeFromCase(expr)
				if eventType != "" {
					position := sa.fileSet.Position(expr.Pos())
					consumer := ConsumerInfo{
						File:         filepath.Base(filename),
						Line:         position.Line,
						HandlerFunc:  sa.findHandlerFunction(caseClause),
						ReceiverType: sa.findReceiverType(filename),
					}

					sa.consumers[eventType] = append(sa.consumers[eventType], consumer)
					fmt.Printf("📥 Found CONSUMER: %s handled in %s:%d\n", eventType, consumer.File, consumer.Line)
				}
			}
		}
	}
}

// Helper methods for AST analysis

func (sa *StaticAnalyzer) isEmitterReceiver(expr ast.Expr) bool {
	// Check various patterns for emitter access
	switch e := expr.(type) {
	case *ast.Ident:
		return strings.Contains(e.Name, "emitter")
	case *ast.SelectorExpr:
		return strings.Contains(e.Sel.Name, "emitter") || sa.isEmitterReceiver(e.X)
	}
	return false
}

func (sa *StaticAnalyzer) extractEventType(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.CompositeLit:
		if ident, ok := e.Type.(*ast.Ident); ok {
			return ident.Name
		}
		if sel, ok := e.Type.(*ast.SelectorExpr); ok {
			return sel.Sel.Name
		}
	}
	return ""
}

func (sa *StaticAnalyzer) extractTypeFromCase(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
}

func (sa *StaticAnalyzer) findEnclosingFunction(node ast.Node, filename string) string {
	// This would require more complex AST traversal to find the containing function
	// For now, return a placeholder
	return "unknown_function"
}

func (sa *StaticAnalyzer) extractPackageName(filename string) string {
	// Extract package name from file path
	dir := filepath.Dir(filename)
	return filepath.Base(dir)
}

func (sa *StaticAnalyzer) isInOnEventMethod(node ast.Node, filename string) bool {
	// This would require checking if we're inside an OnEvent method
	// For now, assume any type switch might be an event handler
	return true
}

func (sa *StaticAnalyzer) findHandlerFunction(caseClause *ast.CaseClause) string {
	// Look for function calls in the case body
	for _, stmt := range caseClause.Body {
		if exprStmt, ok := stmt.(*ast.ExprStmt); ok {
			if call, ok := exprStmt.X.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					return sel.Sel.Name
				}
			}
		}
	}
	return "unknown_handler"
}

func (sa *StaticAnalyzer) findReceiverType(filename string) string {
	// Extract receiver type from filename or context
	// This is a simplified version
	return "UnknownReceiver"
}

// BuildEventMappings converts the analyzed data into StaticEventMapping format
func (sa *StaticAnalyzer) BuildEventMappings() map[string]*StaticEventMapping {
	mappings := make(map[string]*StaticEventMapping)

	// Get all unique event types
	allEvents := make(map[string]bool)
	for eventType := range sa.producers {
		allEvents[eventType] = true
	}
	for eventType := range sa.consumers {
		allEvents[eventType] = true
	}

	// Build mappings for each event type
	for eventType := range allEvents {
		producers := sa.producers[eventType]
		consumers := sa.consumers[eventType]

		// Convert to our expected format
		producedEvents := sa.inferProducedEvents(eventType, producers)
		consumedData := sa.inferConsumedData(eventType, consumers)

		mapping := &StaticEventMapping{
			EventName:   eventType,
			HandlerFunc: sa.getMainHandler(consumers),
			SourceFile:  sa.getMainSourceFile(producers),
			Consumed:    consumedData,
			Produced:    producedEvents,
		}

		mappings[eventType] = mapping
	}

	return mappings
}

func (sa *StaticAnalyzer) inferProducedEvents(eventType string, producers []ProducerInfo) []string {
	// For now, assume the event produces itself
	// In a more sophisticated version, we'd analyze what the handler functions emit
	return []string{eventType}
}

func (sa *StaticAnalyzer) inferConsumedData(eventType string, consumers []ConsumerInfo) []string {
	// For now, assume the event consumes itself
	// In a more sophisticated version, we'd analyze what data the event contains
	return []string{eventType}
}

func (sa *StaticAnalyzer) getMainHandler(consumers []ConsumerInfo) string {
	if len(consumers) > 0 {
		return consumers[0].HandlerFunc
	}
	return "unknown"
}

func (sa *StaticAnalyzer) getMainSourceFile(producers []ProducerInfo) string {
	if len(producers) > 0 {
		return producers[0].File
	}
	return "unknown"
}

// PrintResults shows what the analyzer found
func (sa *StaticAnalyzer) PrintResults() {
	fmt.Printf("\n🎉 Static Analysis Results:\n")
	fmt.Printf("📤 Found %d event types with producers\n", len(sa.producers))
	fmt.Printf("📥 Found %d event types with consumers\n", len(sa.consumers))

	fmt.Printf("\n📊 Event Analysis Summary:\n")
	allEvents := make(map[string]bool)
	for eventType := range sa.producers {
		allEvents[eventType] = true
	}
	for eventType := range sa.consumers {
		allEvents[eventType] = true
	}

	for eventType := range allEvents {
		producers := len(sa.producers[eventType])
		consumers := len(sa.consumers[eventType])
		fmt.Printf("  %s: %d producers, %d consumers\n", eventType, producers, consumers)
	}
}
