import os
from openai import OpenAI

from utils.api_keys import get_api_key
from utils.config import LLM_API_MODEL, LLM_API_BASE

with open(os.path.join(os.path.dirname(__file__), "prompt.txt")) as f:
    prompt = "".join(f.readlines())


def explain_result(query: str, metadata: str):
    client = OpenAI(
        api_key=get_api_key(),
        base_url=LLM_API_BASE,
    )
    try:
        response = client.chat.completions.create(
            model=LLM_API_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                },
                {
                    "role": "assistant",
                    "content": f"好的, 请输入您的数据集和查询, 我将为您解释查询为什么和数据集相关.",
                },
                {
                    "role": "user",
                    "content": f"这是一段查询: {query}. \n"
                    f"这是数据集的元数据: {metadata}",
                },
            ],
        )
        return response.choices[0].message.content

    except Exception as e:
        print(e)
        return "NONE"
