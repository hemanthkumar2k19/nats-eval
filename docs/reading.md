## Few points from documentation

## Stream

- Stream name is case-sensitive
- Subjects bing exactly to one stream -> duplicate causes subjects overlap
- Storage Retention if not configured, stream can take down the server
- The server allows exactly one live change: it swaps Limits and Interest, in either direction. Anything involving WorkQueue is locked — see the pitfall below. And even the allowed swap applies to messages already stored, right away
- Retention to or from WorkQueue is locked after creation

