from __future__ import annotations
import aiohttp
import pydantic
from assemblyline.common.classification import Classification

from . import yaraparse
# from yaramod import (
#     Yaramod,
#     String,
#     PlainString,
#     AndExpression,
#     OrExpression,
#     ParenthesesExpression,
#     StringExpression,
#     OfExpression,
#     GtExpression,
#     GeExpression,
#     LtExpression,
#     LeExpression,
#     EqExpression,
#     StringCountExpression,
#     IntLiteralExpression,
#     StringAtExpression,
#     IdExpression,
#     StringInRangeExpression,
# )


class SearchStatus(pydantic.BaseModel):
    code: str
    finished: bool
    errors: list[str]
    pending_indices: int
    pending_candidates: int
    hits: list[str]
    truncated: bool


class Client:
    def __init__(self, address: str, classification: Classification):
        self.session = aiohttp.ClientSession(base_url=address)
        self.address = address
        self.access_engine = classification

    async def start_search(self, yara_rule: str, access_control: str, archive_only=False) -> SearchStatus:
        # Parse the access string into a set of key words
        access_fields = self.access_engine.fields(access_control)

        # If we only want archived material set the start date to the far future
        start_date = None
        if archive_only:
            start_date = '9000-01-01T00:00:00.000'

        # Send the search request
        result = await self.session.post('/search', json={
            'access': access_fields,
            'query': query_from_yara(yara_rule),
            'yara_signature': yara_rule,
            'start_date': start_date,
            'end_date': None,
        })
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())

    async def search_status(self, code: str) -> SearchStatus:
        # Send the request
        result = await self.session.get('/search/' + code)
        result.raise_for_status()

        # Parse the message
        return SearchStatus(**await result.json())


class QueryExpression:
    def __init__(self, data) -> None:
        if isinstance(data, dict):
            self.data = data
        else:
            raise NotImplementedError(type(data))

    @classmethod
    def literal(cls, some_string: bytes) -> QueryExpression:
        return cls({"Literal": some_string})

    @classmethod
    def and_(cls, *args: QueryExpression) -> QueryExpression:
        if len(args) == 1:
            return args[0]
        return cls(dict(And=[x.data for x in args]))

    @classmethod
    def or_(cls, *args: QueryExpression) -> QueryExpression:
        return cls(dict(Or=[x.data for x in args]))

    @classmethod
    def min_of(cls, howmany: int, *of: QueryExpression) -> QueryExpression:
        raise NotImplementedError()
        # return cls(f"(min {howmany} of ({', '.join(x.query for x in of)}))")


yaraparse.UrsaExpression = QueryExpression


def query_from_yara(yara_rule: str) -> dict:
    rules = yaraparse.parse_yara(yara_rule)

    if len(rules) == 0:
        raise ValueError("A yara rule couldn't be found")
    elif len(rules) == 1:
        return rules[0].parse().data
    else:
        raise ValueError("Only a single yara rule expected")



# def query_from_yara(yara_rule: str) -> dict:
#     """Parse query out from yara rule. Mimicking similar functionality in mquery.
#     I'd rather just import their package, but they don't publish it, and don't have
#     enough metadata (package name) set in their repo to install it directly from there.

#     https://github.com/CERT-Polska/mquery/blob/187eed866c2660f4c142abff3000052435836c15/src/lib/yaraparse.py
#     """
#     yar = Yaramod()
#     raw_rules = yar.parse_string(yara_rule)

#     if len(raw_rules.rules) == 0:
#         raise ValueError("A yara rule couldn't be found")
#     elif len(raw_rules.rules) == 1:
#         return parse_rule(raw_rules.rules[0])
#     else:
#         raise ValueError("Only a single yara rule expected")


# def parse_rule(rule) -> dict:
#     strings = {}
#     anonymous_no = 0

#     for string in rule.strings:
#         if string.identifier == "$":
#             strings[f"anonymous_{anonymous_no}"] = string
#             anonymous_no += 1
#         else:
#             strings[string.identifier] = string

#     engine = yaraparse.RuleParseEngine(strings)


# def parse_condition(rule, strings) -> dict:
#     callback = PARSE_BRANCHES.get(type(rule))
#     if callback:
#         return callback(rule, strings)
#     raise NotImplementedError(type(rule))


# def parse_and_expr(rule, strings) -> dict:
#     left = parse_condition(rule.left_operand, strings)
#     right = parse_condition(rule.right_operand, strings)

#     if left and right:
#         return dict(And=[left, right])
#     elif not left and not right:
#         raise ValueError("Empty and")
#     else:
#         return left or right


# def parse_or_expr(rule, strings) -> dict:
#     left = parse_condition(rule.left_operand, strings)
#     right = parse_condition(rule.right_operand, strings)

#     if left and right:
#         return dict(Or=[left, right])
#     else:
#         raise ValueError("Empty or")


# def parse_paren_expr(rule, strings):
#     return parse_condition(rule.enclosed_expr, strings)


# def parse_str_expr(rule, strings):
#     raise NotImplementedError("parse_str_expr")


# def parse_of_expr(rule, strings):
#     raise NotImplementedError("parse_of_expr")


# def parse_gt_expr(rule, strings):
#     raise NotImplementedError("parse_gt_expr")


# def parse_ge_expr(rule, strings):
#     raise NotImplementedError("parse_ge_expr")


# def parse_lt_expr(rule, strings):
#     raise NotImplementedError("parse_lt_expr")


# def parse_le_expr(rule, strings):
#     raise NotImplementedError("parse_le_expr")


# def parse_eq_expr(rule, strings):
#     raise NotImplementedError("parse_eq_expr")


# def parse_str_count_expr(rule, strings):
#     raise NotImplementedError("parse_str_count_expr")


# def parse_int_lit_expr(rule, strings):
#     raise NotImplementedError("parse_int_lit_expr")


# def parse_str_at_expr(rule, strings):
#     raise NotImplementedError("parse_str_at_expr")


# def parse_id_expr(rule, strings):
#     raise NotImplementedError("parse_id_expr")


# def parse_str_in_expr(rule, strings):
#     raise NotImplementedError("parse_str_in_expr")


# PARSE_BRANCHES = {
#     AndExpression: parse_and_expr,
#     OrExpression: parse_or_expr,
#     ParenthesesExpression: parse_paren_expr,
#     StringExpression: parse_str_expr,
#     OfExpression: parse_of_expr,
#     GtExpression: parse_gt_expr,
#     GeExpression: parse_ge_expr,
#     LtExpression: parse_lt_expr,
#     LeExpression: parse_le_expr,
#     EqExpression: parse_eq_expr,
#     StringCountExpression: parse_str_count_expr,
#     IntLiteralExpression: parse_int_lit_expr,
#     StringAtExpression: parse_str_at_expr,
#     IdExpression: parse_id_expr,
#     StringInRangeExpression: parse_str_in_expr,
# }


# def ursify_string(string: String) -> dict:
#     if string.is_xor:
#         return ursify_xor_string(string)
#     elif string.is_plain:
#         return ursify_plain_string(
#             string.pure_text,
#             is_ascii=string.is_ascii,
#             is_wide=string.is_wide,
#             is_nocase=string.is_nocase,
#         )
#     elif string.is_hex:
#         value_safe = string.pure_text.decode()
#         return ursify_hex(value_safe)
#     elif string.is_regexp:
#         return ursify_regex_string(string)

#     raise ValueError("Missing string")


# def ursify_xor_string(string: PlainString) -> dict:
#     text_ascii = string.pure_text
#     xored_strings: list[dict] = []

#     # TODO implement modifier ranges - https://github.com/CERT-Polska/mquery/issues/100
#     for xor_key in range(256):
#         xored_ascii = xor(text_ascii, bytes([xor_key]))
#         xored_wide = bytes(x ^ xor_key for y in text_ascii for x in [y, 0])

#         if string.is_ascii:
#             xored_strings.append(UrsaExpression.literal(xored_ascii))
#         if string.is_wide:
#             xored_strings.append(UrsaExpression.literal(xored_wide))

#     return UrsaExpression.or_(*xored_strings)
