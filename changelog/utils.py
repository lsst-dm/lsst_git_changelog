import yaml
import requests

def read_repo_yaml():
    url = "https://raw.githubusercontent.com/lsst/repos/refs/heads/main/etc/repos.yaml"
    response = requests.get(url)
    response.raise_for_status()

    # Parse YAML
    data = yaml.safe_load(response.text)

    # Print or use the data
    result = {}
    for key, value in data.items():
        if isinstance(value, dict) and 'url' in value:
            result[key] = value['url']
        else:
            result[key] = value
    return result

def get_repo_list():
    package_list = read_repo_yaml()

    repos = []
    for _, url in package_list.items():
        s = url.split("/")
        repos.append((s[-2], s[-1].removesuffix(".git")))
    return repos