from atlasian import Jira, Confluence
import json
from pathlib import Path

# Config
jira_api_url = "https://gsep.daimler.com/jira/rest/api/2"
confluence_api_url = "https://gsep.daimler.com/confluence/rest/api"
confluence_page_id = "1084478951"


def prepare_cert():
    """
    Prepare certificated for requst
    """
    certs_folder = Path("certs/")
    cert_path = certs_folder / "cert.crt"
    key_path = certs_folder / "key.key"
    cert = (cert_path, key_path)
    return cert


"""
TODO: Prepare function to get secrets from Jenkins storage
"""
with open("tokens.json", "r") as file:
    data = json.loads(file.read())
    jira_token = data['jira']
    confluence_token = data['confluence']
    user_name = data['username']

cert = prepare_cert()
jira = Jira(jira_api_url, user_name, jira_token, cert)
confluence = Confluence(confluence_page_id, confluence_api_url, user_name, confluence_token, cert)

resent_issues = jira.get_recently_updated_release_tasks()
for issue in resent_issues:
    for attachment in issue["attachments"]:
        print(f"Processing {attachment['file_name']}")
        if attachment and confluence.file_eligible_for_upload(attachment['file_name']):
            jira.download_attachment(attachment["download_url"], attachment["file_name"])
            confluence.send_and_label_attachment(attachment['file_name'])
        else:
            print("will not be uploaded")
        print("*" * 40)
