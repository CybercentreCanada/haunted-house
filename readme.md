

# todo

## Integration
- documentation
- configuration env variable substitution

## Important
- azure retry for connection errors
- turn garbage collection back on
- allow out of order insertion

## Nice to have/performance
- add config flag to tune filter memory use
- add tracing
- use more single value structs for better build time error detection
- join query and of literals into block ands
- let index file handle out of order writes
- segment worker database
- use websocket for communication between broker and workers
- use u32 for expiry group