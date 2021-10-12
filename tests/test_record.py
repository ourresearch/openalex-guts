import unittest

import requests_cache
from ddt import ddt, data

from nose.tools import assert_equals
from nose.tools import assert_false
from nose.tools import assert_is_not_none
from nose.tools import assert_not_equals
from nose.tools import assert_true

requests_cache.install_cache('openalex_requests_cache', expire_after=60*60*24*7)  # expire_after is in seconds

# nosetests tests/

# run default open and closed like this:
# nosetests --processes=50 --process-timeout=600 tests/

# tests just hybrid like this
# nosetests --processes=50 --process-timeout=600 -s tests/test_publication.py:TestHybrid

# tests just active one like this
# nosetests --processes=50 --process-timeout=600 -s tests/test_publication.py:TestActive


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)

    def test_something_another_thing(self):
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
