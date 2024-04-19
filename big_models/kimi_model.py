import json
from utils.tool import load_json_data

from openai import OpenAI


def get_result_from_kimi_model(client, history: list, is_ret_json=True, model='moonshot-v1-128k'):
    completion = client.chat.completions.create(
        model=model,
        messages=history,
        temperature=0.3,
    )
    result = completion.choices[0].message.content
    if is_ret_json:
        return json.loads(result)
    else:
        return result


if __name__ == '__main__':
    pass