from typing import Any, Dict, List
import time
import os

import requests
import urllib3
import xml.etree.ElementTree as etree

nifi_base_url = "https://nifi:8443/nifi-api"


def run_nifi_flow():
    urllib3.disable_warnings()

    username = os.environ.get("NIFI_USERNAME")
    password = os.environ.get("NIFI_PASSWORD")
    if username is None or password is None:
        raise KeyError("Environment variables for NiFi not set")

    access_token = __nifi_login(username, password)
    root_id = __nifi_get_root_pg(access_token)["id"]
    template_id = __nifi_import_template(access_token, root_id, "nifi_template.xml")
    template = __nifi_instantiate_template(access_token, root_id, template_id)
    services = __nifi_get_all_controller_services(access_token, root_id)
    for service in services:
        __nifi_enable_controller_service(
            access_token,
            service["id"],
            service["revision"],
        )
    __nifi_schedule_process_group(access_token, root_id)
    put_hdfs_id = [
        x["component"]["id"]
        # search for processors
        for x in template["processors"]
        # if the type of the processor is PutHDFS
        if x["component"]["type"] == "org.apache.nifi.processors.hadoop.PutHDFS"
    ]
    tasks = 0
    while tasks < 3:
        print(f"NiFi: PutHDFS not completed (ran {tasks} times)")
        time.sleep(5)
        tasks = __nifi_check_run_status(access_token, put_hdfs_id[0])


def __nifi_login(username: str, password: str) -> str:
    data = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(
        url=f"{nifi_base_url}/access/token",
        data=data,
        headers=headers,
        verify=False,
    )
    response.raise_for_status()
    return response.text


def __nifi_get_root_pg(access_token: str) -> Any:
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        url=f"{nifi_base_url}/process-groups/root",
        headers=headers,
        verify=False,
    )
    response.raise_for_status()
    return response.json()


def __nifi_import_template(access_token: str, root_id: str, template_path: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.post(
        url=f"{nifi_base_url}/process-groups/{root_id}/templates/upload",
        headers=headers,
        files={"template": open(template_path, "rb")},
        verify=False,
    )
    response.raise_for_status()
    tree = etree.ElementTree(etree.fromstring(response.text))
    id_element = tree.find(".//id")
    if id_element is None:
        raise ValueError("Tag id was not found")
    id = id_element.text
    if id is None:
        raise ValueError("Tag id has no text")
    return id


def __nifi_instantiate_template(
    access_token: str,
    root_id: str,
    template_id: str,
) -> Dict:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    response = requests.post(
        url=f"{nifi_base_url}/process-groups/{root_id}/template-instance",
        headers=headers,
        json={"templateId": template_id, "originX": 0.0, "originY": 0.0},
        verify=False,
    )
    response.raise_for_status()
    return response.json()["flow"]


def __nifi_schedule_process_group(access_token: str, root_id: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.put(
        url=f"{nifi_base_url}/flow/process-groups/{root_id}",
        headers=headers,
        json={"id": root_id, "state": "RUNNING"},
        verify=False,
    )
    response.raise_for_status()


def __nifi_get_all_controller_services(access_token: str, root_id: str) -> List[Any]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get(
        url=f"{nifi_base_url}/flow/process-groups/{root_id}/controller-services",
        headers=headers,
        verify=False,
    )
    response.raise_for_status()
    return response.json()["controllerServices"]


def __nifi_enable_controller_service(
    access_token: str,
    controller_service_id: str,
    service_revision: Any,
):
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.put(
        url=f"{nifi_base_url}/controller-services/{controller_service_id}/run-status",
        headers=headers,
        json={"revision": service_revision, "state": "ENABLED"},
        verify=False,
    )
    response.raise_for_status()


def __nifi_check_run_status(access_token: str, processor_id: str) -> int:
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        url=f"{nifi_base_url}/processors/{processor_id}",
        headers=headers,
        verify=False,
    )
    response.raise_for_status()
    return int(response.json()["status"]["aggregateSnapshot"]["tasks"])
