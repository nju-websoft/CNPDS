from .rank_gpt import Zhipuai
from .reranker import Reranker
from .result import Result
from .rankllm import PromptMode

from utils.config import (
    LLM_API_MODEL,
    LLM_API_BASE,
    LLM_API_KEYS,
    RERANK_STEP_SIZE,
    RERANK_WINDOW_SIZE,
    TB_DESCRIPTIONS,
    TB_DESCRIPTIONS_ID,
    TB_DESCRIPTIONS_DESC,
)
from utils.database import fetch_as_dict

num_passes = 1
top_k_candidates = 20
window_size = RERANK_WINDOW_SIZE
step_size = RERANK_STEP_SIZE
shuffle_candidates = False
print_prompts_responses = False


agent = Zhipuai(
    model=LLM_API_MODEL,
    context_size=4096,
    prompt_mode=PromptMode.RANK_GPT,
    num_few_shot_examples=0,
    keys=LLM_API_KEYS,
    api_base=LLM_API_BASE,
)


def rerank_results(query: str, results: "list[dict]"):
    results = sorted(results, key=lambda x: x["score"], reverse=True)

    descriptions = [
        lst[0] if len(lst) > 0 else {TB_DESCRIPTIONS_DESC: ""}
        for lst in [
            fetch_as_dict(
                f'SELECT * FROM {TB_DESCRIPTIONS} WHERE {TB_DESCRIPTIONS_ID} = {res["datasetid"]}'
            )
            for res in results
        ]
    ]

    retrieved_results = [
        Result(
            query=query,
            hits=[
                {
                    "content": f'{res["content"]}: {descriptions[i][TB_DESCRIPTIONS_DESC]}',
                    "qid": 1,  # any number is fine
                    "docid": res["docid"],
                    "rank": i + 1,
                    "score": res["score"],
                }
                for i, res in enumerate(results)
            ],
        )
    ]
    reranker = Reranker(agent)

    for _ in range(num_passes):
        # print(f"Pass {pass_ct + 1} of {num_passes}:")
        rerank_results = reranker.rerank(
            retrieved_results,
            rank_end=top_k_candidates,
            window_size=min(window_size, top_k_candidates),
            shuffle_candidates=shuffle_candidates,
            logging=print_prompts_responses,
            step=step_size,
        )

        if num_passes > 1:
            retrieved_results = rerank_results
            for r in retrieved_results:
                r.ranking_exec_summary = None

    return rerank_results
