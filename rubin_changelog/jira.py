import requests


class JiraData:
    def __init__(self):
        self._url = f'https://jira.lsstcorp.org/rest/api/2/search'
        self._headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self._perPage = 5000

    def get_tickets(self):
        start_at = 0
        results = dict()
        while True:
            url = self._url + \
                  "?jql=project=DM&startAt=" + str(start_at) + \
                  "&maxResults=" + str(self._perPage) + \
                  "&fields=key,summary"
            res = requests.get(url, headers=self._headers)
            res_json = res.json()
            total = res_json['total']
            start_at = start_at + self._perPage
            for r in res_json['issues']:
                results[r['key']] = r['fields']['summary']
            if start_at > total:
                break
        return results
