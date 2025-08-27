# YodaBuffett - HARD Architecture Principles (Non-Negotiable)

## 1. **API-First Design**
Every service MUST define its API contract BEFORE implementation:

```typescript
// FIRST: Define the contract
// api-contracts/research-service/v1/search.ts
export interface SearchRequest {
  query: string;
  filters: SearchFilters;
  options: SearchOptions;
}

export interface SearchResponse {
  results: SearchResult[];
  metadata: ResponseMetadata;
}

// THEN: Generate OpenAPI/GraphQL schema
// THEN: Implement service
// THEN: Generate client SDKs
```

**Rules:**
- API contracts live in `shared/contracts/`
- Contracts are versioned (v1, v2, etc.)
- Breaking changes require new version
- All fields have explicit types (no `any`)
- Required vs optional clearly marked

## 2. **Pure Functional Core**
Business logic MUST be pure functions with NO side effects:

```python
# GOOD: Pure function
def calculate_roe(net_income: Decimal, equity: Decimal) -> Decimal:
    if equity == 0:
        return Decimal("0")
    return (net_income / equity) * 100

# BAD: Side effects
def calculate_and_save_roe(company_id: str) -> Decimal:
    data = db.fetch(company_id)  # ❌ Side effect
    roe = calculate_roe(data.net_income, data.equity)
    db.save(company_id, roe)     # ❌ Side effect
    return roe
```

**Benefits:**
- Easy to test (no mocks needed)
- Easy to compose
- Easy to parallelize
- No hidden dependencies

## 3. **Hexagonal Architecture**
Core business logic MUST be independent of external systems:

```
┌─────────────────────────────────────┐
│         External World              │
│  (HTTP, Database, Queue, Files)     │
└────────────┬────────────────────────┘
             │ Adapters
┌────────────▼────────────────────────┐
│          Ports (Interfaces)         │
├─────────────────────────────────────┤
│         Core Business Logic         │
│      (Pure, No Dependencies)        │
└─────────────────────────────────────┘
```

**Implementation:**
```typescript
// Core domain (no external dependencies)
interface CompanyAnalyzer {
  analyzeFinancials(data: FinancialData): Analysis;
}

// Port (interface)
interface FilingRepository {
  getFilings(symbol: string): Promise<Filing[]>;
}

// Adapter (implementation)
class PostgresFilingRepository implements FilingRepository {
  // Actual database code here
}
```

## 4. **Immutable Data Structures**
Data MUST be immutable throughout the system:

```typescript
// GOOD: Immutable
interface Filing {
  readonly id: string;
  readonly company: string;
  readonly data: ReadonlyArray<Metric>;
}

// Update by creating new object
const updated = {
  ...original,
  status: 'processed'
};

// BAD: Mutable
filing.status = 'processed'; // ❌ Direct mutation
```

## 5. **Explicit Dependencies**
ALL dependencies MUST be explicit and injected:

```python
# GOOD: Explicit dependencies
class ResearchService:
    def __init__(
        self,
        filing_repo: FilingRepository,
        llm_client: LLMClient,
        vector_store: VectorStore
    ):
        self.filing_repo = filing_repo
        self.llm_client = llm_client
        self.vector_store = vector_store

# BAD: Hidden dependencies
class ResearchService:
    def search(self, query: str):
        db = PostgresClient()  # ❌ Hidden dependency
        llm = OpenAI()        # ❌ Hidden dependency
```

## 6. **Contract Testing**
Services MUST maintain contracts through tests:

```typescript
// Contract test ensures API compatibility
describe('Research Service Contract', () => {
  it('search endpoint matches contract', () => {
    const response = await client.search(validRequest);
    expect(response).toMatchContract(SearchResponseV1);
  });
});
```

## 7. **Event Sourcing for State Changes**
State changes MUST be recorded as events:

```typescript
// Don't just update state
// Record WHAT happened
interface Event {
  id: string;
  type: string;
  timestamp: Date;
  data: unknown;
}

// Examples
FilingProcessedEvent
UserQueryExecutedEvent
PredictionCalculatedEvent
```

## 8. **No Circular Dependencies**
Module dependencies MUST form a directed acyclic graph:

```
shared/types
    ↑
    ├── filing-service
    ├── research-service
    └── prediction-service

✅ Services depend on shared
❌ Services don't depend on each other
❌ Shared doesn't depend on services
```

## 9. **Structured Concurrency**
Concurrent operations MUST be structured and cancellable:

```python
# GOOD: Structured concurrency
async with asyncio.TaskGroup() as tg:
    task1 = tg.create_task(analyze_filing(filing))
    task2 = tg.create_task(get_competitors(company))
    task3 = tg.create_task(fetch_macro_data())
# All tasks cancelled if any fails

# BAD: Fire and forget
asyncio.create_task(process_filing())  # ❌ Untracked
```

## 10. **Fail Fast, Fail Clearly**
Systems MUST fail immediately with clear errors:

```typescript
// GOOD: Fail fast with clear error
if (!filing.extracted_text) {
  throw new ValidationError(
    'Filing missing extracted_text',
    { filingId: filing.id }
  );
}

// BAD: Silent failure or unclear error
if (!filing.extracted_text) {
  return null;  // ❌ Silent failure
  // or
  throw new Error('Error');  // ❌ Unclear
}
```

## 11. **Living Documentation**
Documentation MUST be kept current with code changes:

```markdown
## Documentation Update Requirements

### Every Code Change Must Update:
- [ ] Relevant CLAUDE.md files
- [ ] API contracts if interfaces change
- [ ] Architecture docs if structure changes
- [ ] Roadmap status if milestones reached

### Documentation as Code
- [ ] Documentation changes in same PR as code
- [ ] Broken doc links fail CI/CD pipeline
- [ ] Outdated examples trigger warnings
- [ ] Auto-generated docs from code comments
```

**Documentation Standards:**
- Update docs BEFORE or WITH code changes, never after
- Every service change updates its CLAUDE.md
- Architecture changes update relevant docs/ files
- Roadmap progress tracked in real-time
- Examples and code snippets must always work

## Why These Principles?

These HARD principles ensure:
1. **Composability**: Pure functions + explicit contracts = easy mixing
2. **Testability**: No hidden dependencies = easy testing
3. **Maintainability**: Clear boundaries = local reasoning
4. **Scalability**: Stateless core = horizontal scaling
5. **Flexibility**: Hexagonal architecture = swap implementations
6. **Sustainability**: Living documentation = long-term maintainability

With these principles, adding a new analysis type is just:
1. Define the contract
2. Implement pure business logic
3. Wire up adapters
4. Update documentation
5. Deploy independently

No spaghetti! 🍝