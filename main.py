from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import json
from bson import ObjectId
from datetime import datetime
import re
import os
import tempfile

app = FastAPI()

# Function to generate a random ObjectId
def generate_object_id():
    return str(ObjectId())

# Convert time string (HH:MM:SS or MM:SS) to milliseconds
def time_to_milliseconds(time_str):
    time_parts = list(map(int, re.split(':', time_str)))
    if len(time_parts) == 2:
        minutes, seconds = time_parts
        hours = 0
    elif len(time_parts) == 3:
        hours, minutes, seconds = time_parts
    else:
        raise ValueError(f"Invalid time format: {time_str}")
    return (hours * 3600 + minutes * 60 + seconds) * 1000

# Generate JSON from Excel
def generate_json_from_excel(file_path):
    df = pd.read_excel(file_path)
    current_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    result = [{
        "_id": {"$oid": generate_object_id()},
        "test_id": {"$oid": generate_object_id()},
        "__v": 0,
        "created_at": {"$date": current_date},
        "updated_at": {"$date": current_date},
        "questions": []
    }]

    for _, row in df.iterrows():
        question = {
            "question_id": {"$oid": row["Question ID"]},
            "question_type": row["Type"],
            "overall_statistics": {
                "attempt_percentage": row["Overall Attempt"],
                "accuracy_percentage": row["Overall Accuracy"],
                "p_value": row["Overall P-Value"],
                "average_time_taken": time_to_milliseconds(row["Overall Time Spent"])
            },
            "toppers_statistics": {
                "attempt_percentage": row["Toppers Attempt"],
                "accuracy_percentage": row["Toppers Accuracy"],
                "p_value": row["Toppers P-Value"],
                "average_time_taken": time_to_milliseconds(row["Toppers Time Spent"])
            }
        }
        result[0]["questions"].append(question)
    return result

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/process")
async def process_file(file: UploadFile = File(...)):
    try:
        suffix = os.path.splitext(file.filename)[-1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        json_data = generate_json_from_excel(tmp_path)

        return JSONResponse(content=json_data)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
