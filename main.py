from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd
import json
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime
import re
import os
import uuid

app = FastAPI()

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

def generate_json_from_excel(file_path, test_id):
    df = pd.read_excel(file_path)
    current_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    result = [
        {
            "_id": {"$oid": str(ObjectId())},
            "test_id": {"$oid": test_id},
            "__v": 0,
            "created_at": {"$date": current_date},
            "updated_at": {"$date": current_date},
            "questions": []
        }
    ]

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
async def process_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    test_id: str = Form(...)
):
    try:
        ObjectId(test_id)  # Validate test_id format
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid test_id format. Must be 24-character hex.")

    input_filename = f"temp_{uuid.uuid4().hex}.xlsx"
    output_filename = f"output_{uuid.uuid4().hex}.json"

    with open(input_filename, "wb") as f:
        f.write(await file.read())

    try:
        json_data = generate_json_from_excel(input_filename, test_id)
        with open(output_filename, "w") as f:
            json.dump(json_data, f, indent=4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {e}")
    finally:
        os.remove(input_filename)

    # Delete output file after sending it
    background_tasks.add_task(os.remove, output_filename)
    return FileResponse(output_filename, media_type="application/json", filename="result.json")
