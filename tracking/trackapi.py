from cleanup.util import make_request

MAILTO = "dev@ourresearch.org"


def query_api_by_work_id(work_id: int):
    url = f"https://api.openalex.org/works/W{work_id}"
    params = {
        "mailto": MAILTO,
    }
    r = make_request(url, params=params)
    if r.status_code == 200:
        return r.json()
    else:
        return {
            'status_code': r.status_code,
            'msg': r.text,
        }