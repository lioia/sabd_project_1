from nipyapi import config, templates, canvas, security
from nipyapi.nifi.apis.connections_api import os


def run_nifi_flow():
    username = os.environ.get("NIFI_USERNAME")
    password = os.environ.get("NIFI_PASSWORD")

    # set NiFi host
    config.nifi_config.host = "https://localhost:8443/nifi-api"
    # disable SSL as no certificates are installed
    config.nifi_config.verify_ssl = False

    if username is None or password is None:
        raise KeyError("Environment variables for NiFi not set")
    security.service_login(username=username, password=password)
    upload_response = templates.upload_template(
        pg_id=canvas.get_root_pg_id(),
        template_file="/app/nifi/nifi_template.xml",
    )
    deployed_template = templates.deploy_template(
        pg_id=canvas.get_root_pg_id(),
        template_id=upload_response.id,
        loc_x=0.0,
        loc_y=0.0,
    )

    for controller in canvas.list_all_controllers():
        canvas.schedule_components(controller, scheduled=True)

    for processor in deployed_template.flow.processors:
        canvas.schedule_processor(processor, scheduled=True)
