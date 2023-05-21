

# todo

## Integration
- documentation
- configuration env variable substitution

## Important
- azure retry for connection errors
- protected against incomplete reads of segment extension pointers
- don't accept files near expiry
- turn garbage collection back on

## Nice to have/performance
- add config flag to tune filter memory use
- add tracing
- use more single value structs for better build time error detection
- join query and of literals into block ands