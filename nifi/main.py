import time
import os

import urllib3
from dotenv import load_dotenv

from nifi.nifi_api import (
    check_run_status,
    enable_controller_service,
    get_all_controller_services,
    get_root_pg,
    import_template,
    instantiate_template,
    login,
    schedule_process_group,
)


def main():
    load_dotenv()
    urllib3.disable_warnings()

    username = os.environ.get("USERNAME")
    password = os.environ.get("PASSWORD")
    if username is None or password is None:
        raise KeyError("Environment variables for NiFi not set")

    access_token = login(username, password)
    root_id = get_root_pg(access_token)["id"]
    template_id = import_template(access_token, root_id, "nifi_template.xml")
    template = instantiate_template(access_token, root_id, template_id)
    services = get_all_controller_services(access_token, root_id)
    for service in services:
        enable_controller_service(
            access_token,
            service["id"],
            service["revision"],
        )
    schedule_process_group(access_token, root_id)
    components_ids = [
        x["component"]["id"]
        # search for processors
        for x in template["processors"]
        # if the type of the processor is PutHDFS
        if x["component"]["type"] == "org.apache.nifi.processors.hadoop.PutHDFS"
    ]
    put_hdfs_id = components_ids[0]
    tasks = 0
    while tasks < 4:
        print(f"NiFi: PutHDFS not completed (ran {tasks} times)")
        time.sleep(5)  # wait 5 secs
        tasks = check_run_status(access_token, put_hdfs_id)
