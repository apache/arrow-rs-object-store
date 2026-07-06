# HF S3 gateway — live smoke test results

Test prefix: `object-store-hf-smoke/1783337076226050000`

**13/13 steps passed.**

| Step | Result | Detail |
|---|---|---|
| put (small object) | PASS | put 28 bytes |
| get (full object, verify bytes) | PASS | got 28 bytes, matches |
| get_opts (ranged, 0..5) [cross-host 302 path] | PASS | range 0..5 = b"hello" |
| head (metadata + size) | PASS | size=28 e_tag=Some("\"0c517354ec0a00efbcf40fa10fe0517464c4bef059061f24788b90775ba085ab\"") |
| put_multipart (11 MiB -> 3 parts) | PASS | uploaded 11534336 bytes via multipart |
| head (multipart object size) | PASS | size=11534336 |
| list (recursive, no delimiter) | PASS | 2 objects: ["object-store-hf-smoke/1783337076226050000/large.bin", "object-store-hf-smoke/1783337076226050000/small.txt"] |
| list_with_delimiter | PASS | 2 objects, 0 common_prefixes |
| copy (same namespace) + verify | PASS | copied and verified |
| put_opts PutMode::Create (fresh key) | PASS | create succeeded on fresh key |
| put_opts PutMode::Create (duplicate -> AlreadyExists) | PASS | correctly rejected duplicate (AlreadyExists) |
| delete (single object) | PASS | deleted single object |
| bulk delete (delete_stream / POST ?delete) | PASS | bulk-deleted 3 objects |
