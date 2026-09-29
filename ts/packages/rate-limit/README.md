# @axond/rate-limit

Pre-dispatch limiter. Two modes:

- `isolate` keeps the window in the process. Two replicas can each admit `limit` requests. That is a soft cap.
- `store` updates `axond_ext_ratelimit_window` through `Store.query`. The cap is as strict as that table's transaction. The extension is trusted in this mode because it writes extension-owned rows.

Core does not implement per-tenant admission. This package is the control that replaces it.
