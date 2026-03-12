# Error & Debugging Logs

| Error Name | Object | Error Type | Resolution |
|---|---|---|---|
| Playwright Timeout Error | `itviec_crawler.py` | `TimeoutError` | Switched `wait_until` from `networkidle` to `load`, increased timeout to 60s, and wrapped `networkidle` check in a try-except block to continue if background requests persist. |
