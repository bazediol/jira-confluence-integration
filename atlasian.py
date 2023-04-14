import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path
import json
from bs4 import BeautifulSoup
import re
from datetime import date, timedelta, datetime
import time
from typing import List


def parse_testfile_name(file_name: str) -> dict:
    """
    General function for parsing test results file name
    File name examples:
    "NTG7_Module_Test_RSU_E444.303_2023-03-29_v700.208.1.xlsx"
    "NTG7_Module_Test_Star_2_codd_E444.303_2023-03-29_v700.208.1.xlsx"
    :param file_name: file name
    :return: {
        "star": Star_\d+ or None
        "module": (rsu|hu|codd)
        "full_release": like E444.303_2023-03-29_v700.208.1
        "release": like E444.303
        "date": datetime(yyyy-mm-dd)
        "version": like v700.208.1
    }
    """
    pattern_star = "(Star_\d+)?_?"
    pattern_module = "(rsu|hu|codd)"
    pattern_release = "([A-Z]\d+\.\d+)"
    pattern_date = "(\d{4}-\d{2}-\d{2})"
    pattern_version = "(v\d+\.\d+\.\d+\.?\d?)"
    pattern_full_release = f"({pattern_release}_{pattern_date}_{pattern_version})"
    pattern = "^NTG7_Module_Test_" + \
        pattern_star + \
        f"{pattern_module}_" + \
        pattern_full_release + \
        ".xlsx$"

    """
    Pattern is:
    "^NTG7_Module_Test_(Star_\d+)?_?(rsu|hu|codd)_(([A-Z]\d+\.\d+)_(\d{4}-\d{2}-\d{2})_(v\d+\.\d+\.\d+\.?\d?)).xlsx$"
    """
    pattern = re.compile(pattern, re.IGNORECASE)
    search = re.search(pattern, file_name)
    if not search:
        return None
    result = {
        "star": search.group(1),
        "module": search.group(2),
        "full_release": search.group(3),
        "release": search.group(4),
        "date": datetime.strptime(search.group(5), "%Y-%m-%d"),
        "version": search.group(6)
    }
    return result


class Request:
    """
    Basic class which allows to execute GET, POST and DELETE requests
    with provided base_url, credentials and certificate
    """
    def __init__(self, base_url: str, login: str,
                 token: str, cert: tuple) -> None:
        """
        Instance initiator
        :param base_url: API url which contains base url and api prefix
            example https://gsep.daimler.com/jira/rest/api/2
        :param login: user login
        :param token: user token
        :param cert: (certificate, cert key)
        :retur: None
        """
        self.base_url = base_url
        self.token = token
        self.login = login
        self.auth = HTTPBasicAuth(self.login, self.token)
        self.cert = cert

    def _get_request(self, endpoint: str = None, full_url: str = None,
                     headers: dict = None, params: dict = None) -> requests.Response:
        """
        Perform get request. If full_url is provided, it will be used instead of endpoint value
        :param endpoint: API endpoint path
        :param full_url: full API url
        :param headers: headers
        :param params: params
        :return: requests.Response
        """
        url = full_url if full_url else f"{self.base_url}/{endpoint}"
        response = requests.get(
            url=url,
            auth=self.auth,
            cert=self.cert,
            headers=headers,
            params=params
        )
        if response.status_code not in [200, 204]:
            print(f"Unsuccessfull request to \n {url}")
            print(f"Response: \n {response.text}")
        return response

    def _post_request(self, endpoint: str = None, full_url: str = None,
                      headers: dict = None, data: dict = None, files=None) -> requests.Response:
        """
        Perform post request. If full_url is provided, it will be used instead of endpoint value
        :param endpoint: API endpoint path
        :param full_url: full API url
        :param headers: headers
        :param data: request body
        :files: file as binary - open(file, 'rb')
        :return: requests.Response
        """
        url = full_url if full_url else f"{self.base_url}/{endpoint}"
        response = requests.post(
            url=url,
            auth=self.auth,
            cert=self.cert,
            headers=headers,
            data=data,
            files=files
        )
        if response.status_code not in [200, 204]:
            print(f"Unsuccessfull request to \n {url}")
            print(f"Response: \n {response.text}")
        return response

    def _delete_request(self, endpoint: str = None, full_url: str = None, headers: dict = None, params: dict = None):
        """
        Perform get request. If full_url is provided, it will be used instead of endpoint value
        :param endpoint: API endpoint path
        :param full_url: full API url
        :param headers: headers
        :param params: params
        :return: requests.Response
        """
        url = full_url if full_url else f"{self.base_url}/{endpoint}"
        response = requests.delete(
            url=url,
            auth=self.auth,
            cert=self.cert,
            headers=headers,
            params=params
        )
        if response.status_code not in [200, 204]:
            print(f"Unsuccessfull request to \n {url}")
            print(f"Response: \n {response.text}")
        return response


class Jira(Request):
    "Jira class for interaction with Jira instance"
    def __init__(self, api_url: str, login: str, token: str, cert: tuple, download_path: str = "downloads") -> None:
        """
        Instance initiator
        :param api_url: API url which contains base url and api prefix
            example https://gsep.daimler.com/jira/rest/api/2
        :param login: user login
        :param token: user token
        :param cert: (certificate, cert key)
        :download_path: path to directy where downloads are stored
        :retur: None
        """
        super().__init__(api_url, login, token, cert)
        self.download_path = self._prepare_download_folder(download_path)

    def _prepare_download_folder(self, folder_path: str) -> Path:
        """
        Initiate instance download folder, create if not exists
        :param folder_path: string with folder path
        :return: Path object for downloads folder
        """
        download_folder = Path(folder_path)
        download_folder.mkdir(exist_ok=True, parents=True)
        return download_folder

    def search_issues(self, jql: str) -> List[dict]:
        """
        Search jira issues with provided jql
        :param jql: jira query language string
        :return: dicit with jira issues from response
        """
        headers = {
            "Content-Type": "application/json"
        }
        data = json.dumps({
            "jql": f"{jql}",
            "fields": [
                "summary",
                "attachment"
            ]
        })
        response = self._post_request(endpoint="search", data=data, headers=headers)
        return response.json().get("issues")

    def get_recently_updated_release_tasks(self, days_before: int = 3) -> List[dict]:
        """
        Get release tasks from Jira updated withing last 3 (by default) days
        :param days_before: days from today for searching
        :return: [
            {
                "id": issue id
                "key: issue key
                "attachments: [{file_name, download_url}]
            }
        ]
        """
        updatedDate = date.today() - timedelta(days=days_before)
        jql = f'''
        project = UISWTOOLS
        AND summary ~ "\\\[NTG7\\\] Release build"
        AND type = issue
        AND component in ("ntg7-release-build")
        AND updatedDate >= {updatedDate.strftime("%Y-%m-%d")}
        ORDER BY created DESC
        '''.replace("\n", " ")
        jql = re.sub("\s+", " ", jql)

        issues = [self.parse_issue(issue) for issue in self.search_issues(jql)]
        return issues

    def download_attachment(self, content_url: str, output_name: str) -> None:
        """
        Download attachment by url and save into downloads folder with provided name
        :param content_url: attachment url for download
        :param output_name: name for saved file
        :return: None
        """
        response = self._get_request(full_url=content_url)
        output_path = self.download_path / output_name
        with open(output_path, "wb+") as file:
            file.write(response.content)

    def parse_issue(self, issue: dict) -> dict:
        """
        Exctract only relevant informatin from issue response
        :param issue: issue from response body
        :return: {
                "id": issue id
                "key: issue key
                "attachments: [{file_name, download_url}]
            }
        """
        issue = {
            "id": issue["id"],
            "key": issue["key"],
            "attachments": self.parse_attachments(issue["fields"]["attachment"])
        }
        return issue

    def parse_attachments(self, attahcments: List[dict]) -> List[dict]:
        """
        Exctract only relevant informatin from issue attachments response
        :param attachments: issue from response body
        :return: {
                "file_name": file name in jira
                "download_url": url for download
            }
        """
        result = []
        for attachment in attahcments:
            result.append({
                "file_name": attachment["filename"],
                "download_url": attachment["content"]
            })
        return result

    def download_issue_attachments(self, issue: dict) -> None:
        """
        Download all attachments from Jira issue
        :param: issue from response
        """
        parsed_issue = self.parse_issue(issue)
        for attachment in parsed_issue["attachments"]:
            self.download_attachment(attachment["download_url"], attachment["file_name"])


class Confluence(Request):
    """
    Class for interaction with Confluence page
    """
    def __init__(self, page_id: str, api_url: str, login: str, token: str, cert: tuple) -> None:
        """
        Instance initiator
        :param: confluence page id
        :param api_url: API url which contains base url and api prefix
            example https://gsep.daimler.com/jira/rest/api/2
        :param login: user login
        :param token: user token
        :param cert: (certificate, cert key)
        :retur: None
        """
        super().__init__(api_url, login, token, cert)
        self.page_id = page_id
        self.current_labels = self.get_current_labels()
        self.attachments = self.get_attachments()
        self.attachment_titles = [attachment.get("title") for attachment in self.attachments]

    def get_page_meta(self) -> str:
        """
        Request page matadata
        :return: response body
        """
        response = self._get_request(f"content/{self.page_id}")
        print(response.text)

    def get_page_content(self) -> str:
        """
        Request page html
        :return: page html as string
        """
        response = self._get_request(f"content/{self.page_id}?expand=body.storage")
        return response.json().get('body').get('storage').get('value')

    def get_current_labels(self) -> List[str]:
        """
        Look for <p> blocks in page html with following pattern ".*\s\(current\):$" to collect currently used labes
        <p>Star2 FUP4 (current):</p> will result -> star2_fup4
        :return: list of labels
        """
        page_body = BeautifulSoup(self.get_page_content(), "html.parser")
        tag_strings = [span.string for span in page_body.find_all('p', string=re.compile(".*\s\(current\):$"))]
        pattern = re.compile("^(.*) \(current\).*$")
        labels = [re.search(pattern, value)[1].lower().replace("-", "_").replace(" ", "_") for value in tag_strings]
        if not labels:
            raise UserWarning("Page was parsed, but no currently used labels found")
        print(f"Current labels are: \n{', '.join(labels)}")
        return labels

    def get_attachments(self, request_limit: dict = {"limit": 50}) -> List[dict]:
        """
        Get all page attachments. request_limit used for pagination in requests
        :param request_limit: {"limit": int}
        """
        response = self._get_request(
            endpoint=f"content/{self.page_id}/child/attachment",
            params=request_limit
        )

        data = response.json()
        attachments = data.get("results")

        # Pagination
        next_url = data.get("_links").get("next")
        while next_url:
            response = self._get_request(
                endpoint=next_url.lstrip("/rest/api/")
            )
            data = response.json()
            attachments.extend(data.get("results"))
            next_url = data.get("_links").get("next")

        return attachments

    def search_attachments_by_label(self, labels: List[str]) -> List[dict]:
        """
        Filter only attachments by provided labels list
        :param lablels: list of labes
        :return: list of attachments with requested labels
        """
        labeled_attachments = list(filter(
            lambda attachment: set(labels).issubset(self.parse_attachment_labels(attachment)), self.attachments
        ))
        return labeled_attachments

    def search_attachment_by_name(self, name: str) -> dict:
        """
        Get attachment dict by name
        :param name: attachment name
        :return: attachment dict
        """
        attachments = list(filter(
            lambda attachment: name == attachment.get("title"), self.attachments
        ))
        if attachments:
            return attachments[0]
        else:
            return None

    def parse_attachment_labels(self, attachment: dict) -> List[str]:
        """
        Parse attachment metadata and extract only its label names
        :param attachment: attachment dict
        :return: List["attachment_name]
        """
        label_meta = attachment.get("metadata").get("labels").get("results")
        labels = []
        if label_meta:
            labels = [label["name"] for label in label_meta]
        return labels

    def get_latest_labeled_attachments(self) -> List[str]:
        """
        Get list of attachment names which are labeled as "latest"
        return: List["attachment_name"]
        """
        labels = self.current_labels
        latest_attachements = []
        for label in labels:
            latest_attachements.extend(self.search_attachments_by_label([label, "latest"]))
        return [attachment.get("title") for attachment in latest_attachements]

    def file_exists(self, file_name: str) -> bool:
        "Check if file exists in confluence"
        if file_name in self.attachment_titles:
            return True
        else:
            return False

    def date_is_newer(self, to_be_uploaded: str, existing: str) -> bool:
        """
        From file names like "NTG7_Module_Test_RSU_E444.303_2023-03-29_v700.208.1.xlsx"
        extract date part 2023-03-29
        and compare if uploaded file date is newer than existing
        :param to_be_uploaded: file name of the file which is going to be uploaded
        :param existing: file name of existing versing of the file
        return: True | False
        """
        to_be_uploaded_date = parse_testfile_name(to_be_uploaded)['date']
        existing_date = parse_testfile_name(existing)['date']
        if to_be_uploaded_date > existing_date:
            return True
        else:
            return False

    def file_eligible_for_upload(self, file_name: str) -> bool:
        """
        Check if file is eligible for uploading
        - file name match pattern
        - file is not already uploaded
        - already existing version is older
        :param file_name: name of the file to upload
        """
        if not parse_testfile_name(file_name):
            print("File name doesn't match the pattern")
            return False
        if self.file_exists(file_name):
            print("File exists")
            return False
        predecessor_file = self.get_attachment_predecessor(file_name)
        if not predecessor_file:
            print("No predecessor")
            return True
        if self.date_is_newer(file_name, predecessor_file):
            print("File is newer")
            return True
        else:
            print("Uploaded file is newer")
            return False

    def get_attachment_predecessor(self, file_name: str) -> str:
        """
        Find already uploaded version of the fle marked as "latest"
        For example
        New file is "NTG7_Module_Test_RSU_E444.303_2023-03-29_v700.208.1.xlsx"
        It will look for file which starts with "NTG7_Module_Test_RSU" and marked as "latest"
        :param file_name: name of the file to upload
        :return: predecessor file name
        """
        parsed_file = parse_testfile_name(file_name)
        star = f'{parsed_file["star"]}_' if parsed_file["star"] else ''
        module = parsed_file["module"].upper()
        search_prefix = f"NTG7_Module_Test_{star}{module}"

        predecessor_file = ""
        for latest_attachment in self.get_latest_labeled_attachments():
            if search_prefix in latest_attachment:
                predecessor_file = latest_attachment
        return predecessor_file

    def add_page_attachment(self, file: Path) -> str:
        """
        Upload file to confluence and get its id
        :param file: Path(path/to/object)
        :return: uploaded file id
        """
        print("Sending file...")
        endpoint = f"content/{self.page_id}/child/attachment"
        files = {'file': open(file, "rb")}
        headers = {
            'X-Atlassian-Token': 'no-check'
        }
        response = self._post_request(endpoint=endpoint, headers=headers, files=files)

        return response.json().get("results")[0].get("id")

    def add_label(self, attachment_id: str, label_list: List[str]) -> bool:
        """
        Add lable to attachment.
        Retry 3 times if response.status_code = 500
        :param attachment_id: ID of attachment
        :param label_list: list of labels to be added
        :return: return True if successfull else False
        """
        endpoint = f"content/{attachment_id}/label"
        headers = {
            "Content-Type": "application/json"
        }
        body = []
        for label in label_list:
            body.append({
                "prefix": "global",
                "name": f"{label}"
            })
        response = self._post_request(endpoint=endpoint, headers=headers, data=json.dumps(body))
        if response.status_code == 500:
            retry = 0
            while retry < 3:
                time.sleep(3)
                response = self._post_request(endpoint=endpoint, headers=headers, data=json.dumps(body))
                if response.status_code == 500:
                    retry += 1
        if response.status_code == 200:
            return True
        else:
            return False

    def remove_label(self, attachment_id: str, label: str) -> None:
        """
        Remove specified label from attachment
        :param attachment_id: id of attachmet
        :param label: label name to be removed
        :return: None
        """
        endpoint = f"content/{attachment_id}/label"
        params = {
            "name": label
        }
        self._delete_request(endpoint=endpoint, params=params)

    def send_and_label_attachment(self, file_name: str) -> None:
        """
        Upload attachment and add corresponding labels including "latest", and if successfull:
        if previous version of the file exists (predecessor), remove "latest" label from it
        :param file_name: name of the file to upload
        """
        file_path = Path(f"downloads/{file_name}")
        predecessor_name = self.get_attachment_predecessor(file_name)

        attachment_id = self.add_page_attachment(file_path)

        # Stop if failed to upload the file
        if not attachment_id:
            print(f"Failed to upload file {file_name}")
            return None

        # Prepare labels
        parsed_file_name = parse_testfile_name(file_name)
        module_label = parsed_file_name["module"].lower()
        main_label = ""
        # Use main label with "star" if file name contain "star" prefix
        if parsed_file_name["star"]:
            main_label = [label for label in self.current_labels if "star" in label][0]
        else:
            main_label = [label for label in self.current_labels if "star" not in label][0]

        label_was_added = self.add_label(attachment_id, [main_label, module_label, "latest"])
        if label_was_added and predecessor_name:
            print(f"Previous file version was detected:\n{predecessor_name}")
            predecessor_id = self.search_attachment_by_name(predecessor_name).get("id")
            self.remove_label(predecessor_id, "latest")
