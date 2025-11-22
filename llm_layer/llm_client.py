import requests

LLM_URL = "https://d26621f9e872.ngrok-free.app/v1/chat/completions"
LLM_URL = "https://e6yzfqtids632a-8085.proxy.runpod.net/v1/chat/completions"
def llm_call(prompt: str) -> str:
    payload = {
        "model": "gpt-oss-20b",
        "messages": [{"role": "user", "content": prompt}]
    }
    resp = requests.post(LLM_URL, json=payload)
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    marker = "<|channel|>final<|message|>"
    return content.split(marker, 1)[1].lstrip() if marker in content else content
