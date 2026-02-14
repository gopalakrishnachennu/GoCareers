from typing import List, Dict
import time
import openai

from .llm_pricing import PRICING_PER_1M


def list_openai_models(api_key: str) -> List[str]:
    client = openai.OpenAI(api_key=api_key)
    models = client.models.list()
    return sorted([m.id for m in models.data])


def get_cost_info(model_id: str) -> Dict[str, float]:
    return PRICING_PER_1M.get(model_id, {})


def calculate_cost(model_id: str, prompt_tokens: int, completion_tokens: int) -> Dict[str, float]:
    info = get_cost_info(model_id)
    if not info:
        return {"input": 0.0, "output": 0.0, "total": 0.0}
    cost_input = (prompt_tokens / 1_000_000) * info["input"]
    cost_output = (completion_tokens / 1_000_000) * info["output"]
    return {
        "input": cost_input,
        "output": cost_output,
        "total": cost_input + cost_output,
    }


def sort_models_by_cost(models: List[str]) -> List[str]:
    def cost_key(model_id: str):
        info = PRICING_PER_1M.get(model_id)
        if not info:
            return (float('inf'), model_id)
        return (info['input'] + info['output'], model_id)
    return sorted(models, key=cost_key)
