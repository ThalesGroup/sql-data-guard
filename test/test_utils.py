import json
import logging
import os
from pathlib import Path
from typing import Optional, List

import boto3

_DEFAULT_MODEL_ID = "anthropic.claude-instant-v1"
_PROJECT_FOLDER = Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute()


def get_project_folder() -> str:
    return str(_PROJECT_FOLDER)


def init_creds_from_file():
    file_name = os.path.join(get_project_folder(), "aws", "config")
    if os.path.exists(file_name):
        os.environ["AWS_CONFIG_FILE"] = file_name
        logging.info("AWS config file set")

def get_model_ids() -> List[str]:
    return ["anthropic.claude-instant-v1", "anthropic.claude-v2:1", "anthropic.claude-v3"]


def invoke_llm(system_prompt: Optional[None], user_prompt: str, model_id: str = _DEFAULT_MODEL_ID) -> str:
    logging.info(f"Going to invoke LLM. Model ID: {model_id}")
    bedrock_client = boto3.client(service_name='bedrock-runtime')
    response = bedrock_client.invoke_model(
        body=_format_model_body(user_prompt, system_prompt, model_id),
        modelId=model_id,
    )
    response_json = json.loads(response.get("body").read())
    response_text = _get_response_content(response_json, model_id)
    logging.info(f"Got response from LLM. Response length: {len(response_text)}")
    return response_text


def _format_model_body(prompt: str, system_prompt: Optional[str], model_id: str) -> str:
    if system_prompt is None:
        system_prompt = "You are a SQL generator helper"
    if "claude" in model_id:
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": system_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "max_tokens": 200,
            "temperature": 0.0,
        }
    elif "jamba" in model_id:
        body = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "n": 1
        }
    else:
        raise ValueError(f"Unknown model_id: {model_id}")
    return json.dumps(body)


def _get_response_content(response_json: dict, model_id: str) -> str:
    if "claude" in model_id:
        return response_json["content"][0]["text"]
    elif "jamba" in model_id:
        return response_json["choices"][0]["message"]["content"]
    else:
        raise ValueError(f"Unknown model_id: {model_id}")

