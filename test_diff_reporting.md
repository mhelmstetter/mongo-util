# Test Cases for Improved Difference Reporting

## Scenario 1: Only TTL Differences
- Input: 3 indexes with only TTL differences
- Expected Output: "⏱️  3 TTL difference(s) found - run with --collModTtl to synchronize TTL settings"

## Scenario 2: Mixed Differences
- Input: 2 TTL differences, 3 non-TTL differences
- Expected Output:
  ```
  ❌ Total issues found: 5
      - 2 TTL difference(s) (run with --collModTtl to fix)
      - 3 non-TTL difference(s) requiring manual investigation
  ```

## Scenario 3: Only Non-TTL Differences
- Input: 4 non-TTL differences (unique, sparse, missing indexes)
- Expected Output: "❌ 4 non-TTL index difference(s) found requiring manual investigation"

## Scenario 4: No Differences
- Input: All indexes match
- Expected Output: "✅ All 15 indexes match between source and destination"

## Code Changes Summary

The improvement tracks differences in three categories:
1. `ttlOnlyDiffCount` - Indexes where only the TTL (expireAfterSeconds) differs
2. `otherDiffCount` - Indexes with non-TTL differences (unique, sparse, missing, etc.)
3. `diffCount` - Total count of all differences

The reporting logic now provides:
- Clear indication when only TTL differences exist (easy to fix with --collModTtl)
- Separate counts for TTL vs non-TTL issues when both exist
- Specific message when only non-TTL differences exist (requires manual intervention)
- Positive confirmation when everything matches

This addresses the user's request for more informative error messages that help users understand:
1. What type of problems were found
2. Which problems can be auto-fixed (TTL with --collModTtl)
3. Which problems need manual investigation