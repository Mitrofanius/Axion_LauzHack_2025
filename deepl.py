import requests
url = "https://d26621f9e872.ngrok-free.app/v1/chat/completions"


def llm_call(prompt: str) -> str:
    """
    Placeholder for your LLM invocation.
    Implement this to send `prompt` to the LLM and return the model's raw text response.
    """
    payload = {
        "model": "gpt-oss-20b",
        "messages": [
            {
                "role": "user",
                "role": "user",
                "content": (
    prompt),
            }
        ],
    }
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json()
    full_answer = data["choices"][0]["message"]["content"]
    marker = "<|channel|>final<|message|>"
    if marker in full_answer:
        return full_answer.split(marker, 1)[1].lstrip()
    return full_answer

