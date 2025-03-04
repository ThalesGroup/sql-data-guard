import datetime
import hashlib
import hmac
import http
import json
import logging
import os
from http.client import HTTPSConnection
from pathlib import Path
from typing import Optional, List

_DEFAULT_MODEL_ID = "anthropic.claude-instant-v1"
_PROJECT_FOLDER = Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute()


def get_project_folder() -> str:
    return str(_PROJECT_FOLDER)


def init_env_from_file():
    full_file_name = os.path.join(get_project_folder(), "config", "aws.env.list")
    if os.path.exists(full_file_name):
        logging.info(f"Going to set env variables from file: {full_file_name}")
        with open(full_file_name) as f:
            for line in f:
                key, value = line.strip().split("=")
                os.environ[key] = value


def get_model_ids() -> List[str]:
    return [
        "anthropic.claude-instant-v1",
        "anthropic.claude-v2:1",
        "anthropic.claude-v3",
    ]


def invoke_llm(
    system_prompt: Optional[None], user_prompt: str, model_id: str = _DEFAULT_MODEL_ID
) -> str:
    logging.info(f"Going to invoke LLM. Model ID: {model_id}")
    prompt = _format_model_body(user_prompt, system_prompt, model_id)
    response_json = _invoke_bedrock_model(prompt, model_id)
    response_text = _get_response_content(response_json, model_id)
    logging.info(f"Got response from LLM. Response length: {len(response_text)}")
    return response_text


def _invoke_bedrock_model(prompt_body: dict, model_id: str) -> dict:
    region = os.environ["AWS_DEFAULT_REGION"]
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    if "AWS_SESSION_TOKEN" in os.environ:
        session_token = os.environ["AWS_SESSION_TOKEN"]
        logging.info(f"Session token: {session_token[:4]}")
    else:
        session_token = None

    logging.info(f"Region: {region}. Keys: {access_key[:4]}, {secret_key[:4]}")

    host = f"bedrock-runtime.{region}.amazonaws.com"

    t = datetime.datetime.now(datetime.UTC)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    json_payload = json.dumps(prompt_body)

    hashed_payload = hashlib.sha256(json_payload.encode()).hexdigest()

    canonical_uri = f"/model/{model_id}/invoke"
    canonical_querystring = ""
    canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-date"
    canonical_request = (
        f"POST\n{canonical_uri}\n{canonical_querystring}\n"
        f"{canonical_headers}\n{signed_headers}\n{hashed_payload}"
    )

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/bedrock/aws4_request"
    string_to_sign = (
        f"{algorithm}\n{amz_date}\n{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_service = sign(sign(k_date, region), "bedrock")
    signature = hmac.new(
        sign(k_service, "aws4_request"), string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "X-Amz-Bedrock-Model-Id": model_id,
        "x-amz-date": amz_date,
        "Authorization": f"{algorithm} Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}",
    }
    if session_token:
        headers["X-Amz-Security-Token"] = session_token

    conn = http.client.HTTPSConnection(host)
    try:
        conn.request("POST", canonical_uri, body=json_payload, headers=headers)
        response = conn.getresponse()
        logging.info(f"Response status: {response.status}")
        logging.info(f"Response reason: {response.reason}")
        data = response.read().decode()
        logging.info(f"Response text: {data}")
        return json.loads(data)
    finally:
        conn.close()


def _format_model_body(
    prompt: str, system_prompt: Optional[str], model_id: str
) -> dict:
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
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            "n": 1,
        }
    else:
        raise ValueError(f"Unknown model_id: {model_id}")
    return body


def _get_response_content(response_json: dict, model_id: str) -> str:
    if "claude" in model_id:
        return response_json["content"][0]["text"]
    elif "jamba" in model_id:
        return response_json["choices"][0]["message"]["content"]
    else:
        raise ValueError(f"Unknown model_id: {model_id}")
