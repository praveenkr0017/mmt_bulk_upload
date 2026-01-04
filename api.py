import tempfile
import os
import uuid

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException
)

from fastapi.responses import (
    FileResponse,
    JSONResponse
)

from fastapi.middleware.cors import CORSMiddleware

from bulk_upload.services.employee_processor import process_excel
from bulk_upload.db import create_job_entry
from bulk_upload.config import DB_CRED

app = FastAPI(title="Employee Upload API")

# 🔐 CORS CONFIGURATION
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload-employees")
async def upload_employees(file: UploadFile = File(...)):

    job_id = str(uuid.uuid4())

    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(await file.read())
        temp_path = tmp.name

    try:
        import polars as pl
        df = pl.read_excel(temp_path)
        total_records = df.height
        print(f"[Check] : Total Records found for insertion : {total_records}")
        del df

        try:
            create_job_entry(
                job_id,
                total_records,
                file.filename,
                *DB_CRED
            )
            print(f"[Check] : ✅ Job id created `{job_id}`.")
        except Exception as e:
            print(f"[JobIdEntryError] : ❌ Failed to create job_id `{job_id}` entry in DB due to `{e}`")

        success, failed_df = process_excel(
            file_path=temp_path,
            job_id=job_id
        )

        if success:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "All records inserted successfully",
                    "job_id": job_id
                }
            )

        failed_file = temp_path.replace(".xlsx", "_failed.xlsx")
        failed_df.to_pandas().to_excel(failed_file, index=False)

        return FileResponse(
            path=failed_file,
            filename="failed_records.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        os.remove(temp_path)