from fastapi import APIRouter

from .context_builder import build_context
from .prompt_builder import build_generate_sql_prompt
from .provider_openai import generate_sql_response
from .response_parser import parse_generate_sql_response
from .schemas import GenerateSQLRequest, GenerateSQLResponse
from .sql_validator import validate_generated_sql, validate_sql_with_duckdb

router = APIRouter(prefix="/api/ai", tags=["AI"])


def _get_dataset_source_path(dataset: str):
    try:
        from app.main import dataset_source_path
    except Exception:
        from main import dataset_source_path
    return dataset_source_path(dataset)


@router.post("/generate_sql", response_model=GenerateSQLResponse)
def generate_sql(payload: GenerateSQLRequest) -> GenerateSQLResponse:
    try:
        context = build_context(
            dataset_name=payload.dataset,
            dataset_source_path_fn=_get_dataset_source_path,
            max_sample_rows=5,
        )

        prompt = build_generate_sql_prompt(
            context=context,
            question=payload.question,
        )

        model_output = generate_sql_response(prompt)
        parsed = parse_generate_sql_response(model_output)

        if parsed["status"] == "ok":
            is_valid, validation_message = validate_generated_sql(parsed["sql"])
            if not is_valid:
                return GenerateSQLResponse(
                    status="error",
                    dataset=payload.dataset,
                    question=payload.question,
                    sql="",
                    message=validation_message,
                    warnings=parsed["warnings"],
                )

            duck_ok, duck_message = validate_sql_with_duckdb(
                sql=parsed["sql"],
                dataset_name=payload.dataset,
                dataset_source_path_fn=_get_dataset_source_path,
            )
            if not duck_ok:
                return GenerateSQLResponse(
                    status="error",
                    dataset=payload.dataset,
                    question=payload.question,
                    sql="",
                    message=duck_message,
                    warnings=parsed["warnings"],
                )


        return GenerateSQLResponse(
            status=parsed["status"],
            dataset=payload.dataset,
            question=payload.question,
            sql=parsed["sql"],
            message=parsed["message"],
            warnings=parsed["warnings"],
        )

    except Exception as e:
        return GenerateSQLResponse(
            status="error",
            dataset=payload.dataset,
            question=payload.question,
            sql="",
            message=f"DEBUG ERROR: {type(e).__name__}: {e}",
            warnings=[],
        )