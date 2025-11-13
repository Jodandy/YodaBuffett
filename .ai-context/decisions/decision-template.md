# AI Technical Decision Log Template

**File naming**: `[domain]-[decision-topic].md`  
**Example**: `analytics-performance-optimization.md`, `database-architecture-choices.md`

## Decision Summary
**Decision**: [Clear, specific decision that was made]  
**Date**: [YYYY-MM-DD]  
**AI Assistant**: [Your name/model]  
**Domain**: [Primary domain affected]  
**Status**: [Decided/Implemented/Superseded]

## Context and Problem
**Situation**: [What problem or choice necessitated this decision?]  
**Constraints**: [Technical, business, or resource constraints that affected options]  
**Stakeholders**: [Who/what is affected by this decision]  
**Timeline**: [Any time pressures or deadlines]

### Background Information
- **Current State**: [How things work now, if applicable]
- **Performance Requirements**: [Specific metrics or targets]
- **Integration Requirements**: [How this must work with other systems]
- **Future Considerations**: [Known upcoming needs this decision should account for]

## Options Considered

### Option 1: [Option Name]
**Description**: [Clear explanation of this approach]  
**Pros**:
- [Specific advantage with technical reasoning]
- [Another advantage with measurable benefit]
- [Cost/complexity advantage]

**Cons**:
- [Specific disadvantage or limitation]
- [Another downside with impact assessment]
- [Risk or complexity concern]

**Implementation Complexity**: [High/Medium/Low with explanation]  
**Performance Impact**: [Expected performance characteristics]  
**AI Development Suitability**: [How well this works with AI-assisted development]

### Option 2: [Option Name]
**Description**: [Clear explanation of this approach]  
**Pros**:
- [Advantages with reasoning]

**Cons**:
- [Disadvantages with impact]

**Implementation Complexity**: [Level with explanation]  
**Performance Impact**: [Expected characteristics]  
**AI Development Suitability**: [AI development considerations]

### Option 3: [Option Name] (if applicable)
[Follow same format...]

## Decision Made

**Chosen Option**: [Selected option with brief justification]

### Primary Reasoning
1. **[Key Factor 1]**: [Why this was most important and how chosen option addresses it]
2. **[Key Factor 2]**: [Another critical consideration and how it influenced choice]
3. **[Key Factor 3]**: [Third major factor in decision]

### Trade-offs Accepted
- **[Trade-off 1]**: [What we gave up and why it was acceptable]
- **[Trade-off 2]**: [Another compromise made and reasoning]

### Risk Mitigation
- **[Risk 1]**: [Identified risk and how it will be managed]
- **[Risk 2]**: [Another risk and mitigation strategy]

## Implementation Guidance for AI Assistants

### Technical Implementation Notes
```python
# Example code pattern for this decision
[Code example showing how to implement the chosen approach]

# Key points to remember:
# - [Important implementation detail]
# - [Performance consideration]  
# - [Integration requirement]
```

### File Organization
- **Primary Implementation**: `[file_path]` - [Description of what goes here]
- **Tests**: `[test_file_path]` - [Testing approach]
- **Documentation**: `[doc_file]` - [What documentation to update]

### Integration Points
- **[System/Domain 1]**: [How this decision affects integration with other parts]
- **[System/Domain 2]**: [Another integration consideration]
- **Database Changes**: [Any schema or query pattern changes needed]

### Performance Monitoring
- **Metrics to Track**: [Specific measurements to validate decision success]
- **Alerting**: [What performance degradation should trigger alerts]
- **Optimization Opportunities**: [Future improvements that build on this decision]

## Results and Validation

### Success Criteria
- [ ] **[Criterion 1]**: [Measurable outcome that validates decision success]
- [ ] **[Criterion 2]**: [Another success metric]
- [ ] **[Criterion 3]**: [Third validation point]

### Actual Results (to be updated post-implementation)
**Performance**: [Actual vs expected performance]  
**Development Velocity**: [Impact on AI-assisted development speed]  
**Maintenance Overhead**: [Ongoing complexity introduced]  
**Integration Success**: [How well it worked with other systems]

### Lessons Learned
- **What Worked Well**: [Successful aspects of the implementation]
- **What Was Challenging**: [Unexpected difficulties encountered]
- **Would Do Differently**: [Improvements for similar future decisions]

## Related Decisions
- **[Previous Decision]**: [Link to related decision log with brief explanation]
- **[Dependent Decision]**: [Decision that depends on this one]
- **[Alternative Explored]**: [Related option that was considered separately]

## Future Review Points
**Review Date**: [When to reassess this decision]  
**Triggers for Review**: [Conditions that should prompt reconsideration]
- Performance targets not met after [timeframe]
- Major architectural changes in [related system]
- New [technology/constraint/requirement] emerges

---

## For AI Assistants: Using This Template

### Before Making Technical Decisions:
1. **Check existing decisions**: Search `decisions/` directory for similar choices
2. **Review patterns**: Check `patterns/` for established successful approaches  
3. **Consider domain impact**: Review relevant domain `__domain__.md` for context
4. **Validate with architecture**: Ensure alignment with `ARCHITECTURE_MAP.md`

### During Decision Process:
1. **Document options thoroughly**: Future AI assistants need full context
2. **Be specific about trade-offs**: Vague reasoning doesn't help future decisions
3. **Include implementation guidance**: Technical details that enable quick implementation
4. **Consider AI development impact**: How does this affect future AI-assisted work?

### After Implementation:
1. **Update results section**: Actual performance vs expectations
2. **Update domain documentation**: Reflect decision impact in relevant `__domain__.md`
3. **Create pattern documentation**: If this becomes a reusable approach
4. **Set review reminders**: Schedule decision validation