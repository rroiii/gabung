from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os
import uvicorn

app = FastAPI()
FILES_DIR = os.path.abspath("files")

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.abspath(os.path.join(FILES_DIR, filename))
    if not file_path.startswith(FILES_DIR) or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found or access denied")
    return FileResponse(file_path, filename=filename)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)