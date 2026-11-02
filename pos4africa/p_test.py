import pytest
from typing import Sequence, Mapping
from pydantic import BaseModel, ConfigDict

# --- Minimal implementation setup to run the tests ---
class MatchedResults(BaseModel):
    model_config = ConfigDict(frozen=True)
    comment: str
    count: int
    pos_customer_id: int | None
    customer_found: bool

class CustomerMatcher:
    def match_comments_to_customers(
        self, comments: Sequence[dict], customers: Mapping[str, Mapping]
    ) -> Sequence[MatchedResults]:
        return self.__match_comments_to_customers(comments=comments, customers=customers)
        
    def __match_comments_to_customers(
        self, comments: Sequence[dict], customers: Mapping[str, Mapping]
    ) -> Sequence[MatchedResults]:
        if not comments or not customers:
            return []
        if not isinstance(comments, Sequence):
            raise TypeError("`comments` must be a sequence of dictionaries.")
        if not isinstance(customers, Mapping):
            raise TypeError("`customers` must be a dictionary-like mapping.")
        
        customer_id_map: dict[str, int] = {}
        results: list[MatchedResults] = []

        for info in customers.values():
            first = info.get("first_name") or ""
            last = info.get("last_name") or ""
            fullname = f"{first} {last}".strip().upper()
            if fullname: 
                customer_id_map[fullname] = info["pos_customer_id"]

        for item in comments:
            if "comment" not in item or not isinstance(item["comment"], str): 
                continue
            comment_str: str = item["comment"]
            lookup_key = comment_str.strip().upper()
            matched_found_id = customer_id_map.get(lookup_key, None)

            results.append(MatchedResults(
                comment=comment_str, 
                count=item.get("count", 0),
                pos_customer_id=matched_found_id,
                customer_found=matched_found_id is not None
            ))
        return results


# --- Pytest Fixture ---
@pytest.fixture
def matcher():
    return CustomerMatcher()


# --- Test Cases ---

def test_successful_match(matcher):
    """Test that valid comments match with customer names regardless of casing."""
    customers = {
        "cust1": {"first_name": "John", "last_name": "Doe", "pos_customer_id": 101},
        "cust2": {"first_name": "Alice", "last_name": "Smith", "pos_customer_id": 102}
    }
    comments = [
        {"comment": "john doe", "count": 2},       # Lowercase match
        {"comment": "ALICE SMITH", "count": 5},   # Uppercase match
        {"comment": "No Match", "count": 1}       # Unmatched comment
    ]

    results = matcher.match_comments_to_customers(comments, customers)

    assert len(results) == 3
    
    # Verify John Doe match
    assert results[0].comment == "john doe"
    assert results[0].pos_customer_id == 101
    assert results[0].customer_found is True
    assert results[0].count == 2

    # Verify Alice Smith match
    assert results[1].pos_customer_id == 102
    assert results[1].customer_found is True

    # Verify Unmatched item
    assert results[2].pos_customer_id is None
    assert results[2].customer_found is False


def test_empty_inputs_return_empty_list(matcher):
    """Test that empty inputs elegantly return an empty list."""
    customers = {"cust1": {"first_name": "John", "pos_customer_id": 101}}
    
    assert matcher.match_comments_to_customers([], customers) == []
    assert matcher.match_comments_to_customers([{"comment": "test"}], {}) == []


def test_invalid_and_missing_keys_in_comments(matcher):
    """Verify the validation check skips broken comment items without crashing."""
    customers = {"cust1": {"first_name": "John", "last_name": "Doe", "pos_customer_id": 101}}
    comments = [
        {"count": 3},                      # Missing "comment" key entirely
        {"comment": 12345, "count": 1},     # "comment" is an integer, not a string
        {"comment": "John Doe", "count": 1} # Valid item
    ]

    results = matcher.match_comments_to_customers(comments, customers)

    # It should skip the first two entirely, returning only 1 output result
    assert len(results) == 1
    assert results[0].pos_customer_id == 101


def test_type_error_exceptions(matcher):
    """Test that passing wrong datatypes triggers standard TypeErrors."""
    with pytest.raises(TypeError, match="`comments` must be a sequence"):
        matcher.match_comments_to_customers("not-a-sequence", {})

    with pytest.raises(TypeError, match="`customers` must be a dictionary-like mapping"):
        matcher.match_comments_to_customers([], ["not", "a", "mapping"])
