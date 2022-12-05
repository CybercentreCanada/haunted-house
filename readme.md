

# todo

## Important
- break ingest batches up
- chunk input to yara jobs
- don't save in expired/nearly expired groups
- add garbage collecting
- worker loss detection/task timeout

## Nice to have/performance
- add tracing
- use blob cache for indexing to prevent upload+delete immidately followed by download
- keep a cache of which indices are assigned to what worker
- task retry
- result/error submit retrying