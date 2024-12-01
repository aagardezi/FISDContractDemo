import functions_framework
import os

import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, FinishReason
import vertexai.preview.generative_models as generative_models
from google.cloud import bigquery
from datetime import datetime
import google.auth



def get_project_id():
  """Gets the current GCP project ID.

  Returns:
    The project ID as a string.
  """

  try:
    _, project_id = google.auth.default()
    return project_id
  except google.auth.exceptions.DefaultCredentialsError as e:
    print(f"Error: Could not determine the project ID. {e}")
    return None

#TODO: Get the right dataset id and table id for your bq instance 
# and set it as an env variable when creating the function
PROJECT_ID = get_project_id()
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def send_to_gemini_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    mime_type = data["contentType"]


    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Mime-Type: {mime_type}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    vertexai.init(project=PROJECT_ID, location="us-central1")
    #TODO: Look at the spec here and see how its ensuring json output. Do you need that?
    # model = GenerativeModel(
    #   "gemini-1.5-flash-001",generation_config={"response_mime_type": "application/json"}
    # )

    model = GenerativeModel(
      "gemini-1.5-flash-001"
    )    

    file1 = Part.from_uri(mime_type=mime_type, uri=f"gs://{bucket}/{name}")
    text1 = """Can you summarise the content of this file"""

    responses = model.generate_content(
      [file1, text1],
      generation_config=generation_config,
      safety_settings=safety_settings,
      stream=False,
    )

    fileresponse = ''  
    fileresponse = responses.candidates[0].content.parts[0].text
    
    #This code connects to big query and send data
    # client = bigquery.Client()

    # timestamp = datetime.utcnow().isoformat()

    # data = [{"filepath": f"gs://{bucket}/{name}", "details": fileresponse, "timestamp": timestamp}]

    # errors = client.insert_rows_json(
    #   f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", data)
    # print(errors)

    print(fileresponse)




generation_config = {
    "max_output_tokens": 8192,
    "temperature": 1,
    "top_p": 0.95,
    "response_mime_type": "application/json"
}

safety_settings = {
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
}
    
