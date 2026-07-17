from __future__ import annotations

import os

import uvicorn


if __name__ == "__main__":
    host = os.getenv("SEARCH_API_HOST", "0.0.0.0")
    port = int(os.getenv("SEARCH_API_PORT", "8010"))
    uvicorn.run("search_app.main:app", host=host, port=port, reload=False)
