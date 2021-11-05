# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# HAP removed titles
FIRST_NAME_TITLES = set([
])
"""
When these titles appear with a single other name, that name is a first name, e.g.
"Sir John", "Sister Mary", "Queen Elizabeth".
"""

#: **Cannot include things that could also be first names**, e.g. "dean".
#: Many of these from wikipedia: https://en.wikipedia.org/wiki/Title.
#: The parser recognizes chains of these including conjunctions allowing 
#: recognition titles like "Deputy Secretary of State".

# HAP removed titles
TITLES = FIRST_NAME_TITLES | set([
])
