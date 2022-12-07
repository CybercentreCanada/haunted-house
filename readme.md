

# todo

## Important
- worker loss detection/task timeout
- filter or bundle large response sets

## Nice to have/performance
- use blob cache for indexing to prevent upload+delete immidately followed by download
- let work requests block to see if work becomes available
- add config flag to tune filter memory use
- add tracing
- keep a cache of which indices are assigned to what worker
- task retry
- result/error submit retrying
- use more single value structs for better build time error detection
