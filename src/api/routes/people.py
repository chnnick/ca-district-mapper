from fastapi import APIRouter, Depends, HTTPException

from src.api.deps import get_db
from src.reports.queries import get_districts_for_person

router = APIRouter()


@router.get("/people/{person_id}/districts")
def get_person_districts(
    person_id: str,
    as_of_date: str | None = None,
    conn=Depends(get_db),
):
    """JSON lookup: a single person's CD/SD/AD/BOE assignments by source-CSV id."""
    result = get_districts_for_person(conn, person_id, as_of_date)
    if result is None:
        raise HTTPException(404, detail=f"Person id not found: {person_id}")
    return result
