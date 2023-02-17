

# todo

## Integration
- documentation
- configuration env variable substitution

## Important
- only accept results from the right worker
- azure retry for connection errors

## Nice to have/performance
- add config flag to tune filter memory use
- more efficent way to monitor search progress
- add tracing
- keep a cache of which indices are assigned to what worker
- task retry
- use more single value structs for better build time error detection
- worker that runs without downloading the entire filter file
