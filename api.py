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
# from .services.employee_processor import process_excel
from bulk_upload.services.employee_processor import process_excel

app = FastAPI(title="Employee Upload API")

@app.post("/upload-employees")
async def upload_employees(file: UploadFile = File(...)):

    job_id = str(uuid.uuid4())

    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(await file.read())
        temp_path = tmp.name

    try:
        # Process Excel
        success, failed_df = process_excel(temp_path)

        if success:
            return JSONResponse(
                status_code=200,
                content={"message": "All records inserted successfully"}
            )

        # If failed → create Excel
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
