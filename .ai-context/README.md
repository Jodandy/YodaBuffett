# AI Development Context System

## Purpose
This directory contains AI-maintained context files that enable seamless handoffs between AI assistants and efficient resumption of development work.

## Directory Structure
```
.ai-context/
├── README.md              # This file - explains the system
├── current-work.md         # Active development context (always current)
├── sessions/               # Individual work session logs
│   ├── 2025-01-12-analytics-optimization.md
│   ├── 2025-01-13-market-data-feeds.md
│   └── [date]-[domain]-[topic].md
├── decisions/              # Technical decision logs with reasoning
│   ├── analytics-performance-optimization.md
│   ├── database-architecture-choices.md
│   └── [domain]-[decision-topic].md
└── patterns/               # Successful patterns and anti-patterns
    ├── yodabuffett-development-patterns.md
    ├── debugging-approaches.md
    └── [pattern-category].md
```

## For AI Assistants: How to Use This System

### 1. Starting a New Work Session
1. **Read `current-work.md`** to understand active development context
2. **Create new session file** in `sessions/` with format: `YYYY-MM-DD-[domain]-[topic].md`
3. **Update `current-work.md`** with your session details

### 2. During Development Work
1. **Update session file** with progress, findings, and next steps
2. **Log important decisions** in `decisions/` if architectural choices are made
3. **Update `current-work.md`** when switching focus or completing work

### 3. Ending a Work Session
1. **Finalize session notes** with clear handoff context for next AI
2. **Update `current-work.md`** with current state and immediate next steps
3. **Create decision log** if significant technical choices were made

### 4. Before Making Technical Decisions
1. **Check `decisions/`** to see if similar decisions were already made
2. **Review `patterns/`** for established successful approaches
3. **Create new decision log** if making novel architectural choice

## Maintenance Guidelines

### Session Files Should Include:
- Clear session goal and context loaded
- Work completed (with file references and line numbers)
- Current blockers or challenges
- Specific handoff notes for next AI assistant

### Decision Logs Should Include:
- Problem context and constraints
- Options considered with pros/cons
- Decision made and reasoning
- Implementation notes for future AI assistants

### Pattern Files Should Include:
- Successful approaches that work well in YodaBuffett
- Common pitfalls and how to avoid them
- Code patterns that AI assistants should follow
- Performance optimization strategies that have proven effective

## Integration with Domain Documentation

This AI context system complements the domain `__domain__.md` files:
- **Domain docs**: Long-term, structural knowledge about each domain
- **AI context**: Short-term, tactical knowledge about current work

Both should be maintained and cross-referenced for maximum AI effectiveness.