import re
import unidecode
import unicodedata
from collections import namedtuple
from rapidfuzz import fuzz

from app import db
from const import ROR_COUNTRIES_LIST

class RORGapInstitution(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "ror_institutions_for_parser_gap"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    ror_id = db.Column(db.Text)
    display_name = db.Column(db.Text)

class RORStrategy:
    strategy = "affiliation-single-search"
    task = "affiliation-matching"
    description = "Heuristic-based strategy; it performs a single candidate search against ES and uses a number of heuristics to validate the candidates."
    default = False

    index = "search-ror-institutions-v2"
    max_candidates = 200
    min_score = 96

    def __init__(self):
        self.countries = ROR_COUNTRIES_LIST

    def match(self, input_data, candidates):
        aff_countries = self.get_countries(input_data)
        candidates = [c for c in candidates if c["_source"]["status"] == "active"]

        candidates = [self.score(input_data, c) for c in candidates]
        candidates = [c for c in candidates if c.score >= self.min_score]

        matched = self.choose_candidate(input_data, candidates)
        if matched is None:
            return []
        if (
            aff_countries
            and self.to_region(matched.candidate["_source"]["country"])
            not in aff_countries
        ):
            return []
        return [
            {
                "id": matched.candidate["_id"],
                "confidence": min(12, matched.candidate["_score"]) / 12,
                "strategies": [self.strategy],
            }
        ]

    def check_latin_chars(self, s):
        for ch in s:
            if ch.isalpha():
                if "LATIN" not in unicodedata.name(ch):
                    return False
        return True

    def normalize(self, s):
        """Normalize string for matching."""

        if self.check_latin_chars(s):
            s = re.sub(r"\s+", " ", unidecode.unidecode(s).strip().lower())
        else:
            s = re.sub(r"\s+", " ", s.strip().lower())
        s = re.sub(
            "(?<![a-z])univ$",
            "university",
            re.sub(
                r"(?<![a-z])univ[\. ]",
                "university ",
                re.sub(r"(?<![a-z])u\.(?! ?[a-z]\.)", "university ", s),
            ),
        )
        s = re.sub(
            "(?<![a-z])lab$",
            "laboratory",
            re.sub("(?<![a-z])lab[^a-z]", "laboratory ", s),
        )
        s = re.sub(
            "(?<![a-z])inst$",
            "institute",
            re.sub("(?<![a-z])inst[^a-z]", "institute ", s),
        )
        s = re.sub(
            "(?<![a-z])tech$",
            "technology",
            re.sub("(?<![a-z])tech[^a-z]", "technology ", s),
        )
        s = re.sub(r"(?<![a-z])u\. ?s\.", "united states", s)
        s = re.sub("&", " and ", re.sub("&amp;", " and ", s))
        s = re.sub("^the ", "", s)
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s

    CandidateMatch = namedtuple(
        "CandidateMatch",
        ["candidate", "name", "score", "start", "end"],
    )

    def score(self, aff, candidate):
        best = self.CandidateMatch(
            candidate=candidate, name="", score=0, start=-1, end=-1
        )
        for candidate_name in candidate["_source"]["names"]:
            name = candidate_name["name"][0]
            if (
                name.lower() in ["university school", "university hospital"]
                or len(name) >= len(aff) + 4
                or len(name) < 5
                or " " not in name
            ):
                continue
            alignment = fuzz.partial_ratio_alignment(
                self.normalize(aff), self.normalize(name)
            )
            if alignment.score > best.score:
                best = self.CandidateMatch(
                    candidate=candidate,
                    name=name,
                    score=alignment.score,
                    start=alignment.src_start,
                    end=alignment.src_end,
                )
        return best

    def is_better(self, aff, candidate, other):
        score = 0
        if "univ" in candidate.name.lower() and "univ" not in other.name.lower():
            score += 1
        if "univ" not in candidate.name.lower() and "univ" in other.name.lower():
            score -= 1
        c_diff = abs(len(candidate.name) - len(aff))
        o_diff = abs(len(other.name) - len(aff))
        if o_diff - c_diff > 4:
            score += 1
        if c_diff - o_diff > 4:
            score -= 1
        if candidate.start > other.end:
            score += 1
        if other.start > candidate.end:
            score -= 1
        if candidate.score > 99 and other.score < 99:
            score += 1
        if candidate.score < 99 and other.score > 99:
            score -= 1
        return score > 0

    def rescore(self, aff, candidates):
        new_scores = []
        for candidate in candidates:
            ns = 0
            for other in candidates:
                if self.is_better(aff, candidate, other):
                    ns += 1
            new_scores.append(ns)
        return [c._replace(score=ns) for c, ns in zip(candidates, new_scores)]

    def last_non_overlapping(self, candidates):
        matched = None
        for candidate in candidates:
            overlap = False
            for other in candidates:
                if candidate.candidate["_id"] == other.candidate["_id"]:
                    continue
                if (
                    candidate.start <= other.start <= candidate.end
                    or candidate.start <= other.end <= candidate.end
                    or other.start <= candidate.start <= other.end
                    or other.start <= candidate.end <= other.end
                ):
                    overlap = True
            if not overlap:
                matched = candidate
        return matched

    def choose_candidate(self, aff, candidates):
        if not candidates:
            return None

        if len(candidates) == 1:
            return candidates[0]

        rescored = self.rescore(aff, candidates)
        top_score = max([c.score for c in rescored])
        top_scored = [c for c in rescored if c.score == top_score]

        if len(top_scored) == 1:
            return top_scored[0]

        return self.last_non_overlapping(top_scored)

    def to_region(self, c):
        return {
            "GB": "GB-UK",
            "UK": "GB-UK",
            "CN": "CN-HK-TW",
            "HK": "CN-HK-TW",
            "TW": "CN-HK-TW",
            "PR": "US-PR",
            "US": "US-PR",
        }.get(c, c)

    def get_country_codes(self, string):
        string = unidecode.unidecode(string).strip()
        lower = re.sub(r"\s+", " ", string.lower())
        lower_alpha = re.sub(r"\s+", " ", re.sub("[^a-z]", " ", string.lower()))
        alpha = re.sub(r"\s+", " ", re.sub("[^a-zA-Z]", " ", string))
        codes = []
        for code, name in self.countries:
            if re.search("[^a-z]", name):
                score = fuzz.partial_ratio(name, lower)
            elif len(name) == 2:
                score = max([fuzz.ratio(name.upper(), t) for t in alpha.split()] + [0])
            else:
                score = max([fuzz.ratio(name, t) for t in lower_alpha.split()] + [0])
            if score >= 90:
                codes.append(code.upper())
        return list(set(codes))

    def get_countries(self, string):
        codes = self.get_country_codes(string)
        return [self.to_region(c) for c in codes]
