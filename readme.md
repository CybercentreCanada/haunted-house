

# todo

- also check maximum files per filter against the chunk size and chunk index limit

## Integration
- documentation

## Nice to have/performance
- add config flag to tune filter memory use
- use more single value structs for better build time error detection
- join query and of literals into block ands
- simplify communication with workers, especially reduce/remove job polling
- retry file downloads and yara jobs
