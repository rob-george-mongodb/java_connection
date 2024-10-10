import requests
from requests.auth import HTTPDigestAuth
import time
import os
import sys
from dotenv import load_dotenv


args = sys.argv
if len(args) > 0:
    dotenv_path = args[1]
else:
    dotenv_path = None
print(dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

# Atlas API Configuration
BASE_URL = os.getenv("BASE_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
CLUSTER_NAME = os.getenv("CLUSTER_NAME")
PUBLIC_KEY = os.getenv("PUBLIC_KEY")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

# Create a session with Digest Authentication
session = requests.Session()
session.auth = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)
session.headers.update({"Accept": "application/vnd.atlas.2024-08-05+json"})


def get_current_cluster_config():
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/clusters/{CLUSTER_NAME}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def update_cluster_size(new_size):
    url = f"{BASE_URL}/api/atlas/v2/groups/{PROJECT_ID}/clusters/{CLUSTER_NAME}"

    # Get the current configuration
    current_config = get_current_cluster_config()

    # Prepare the update payload
    payload = {}

    # Update the instance size in all regionConfigs
    for spec in current_config.get("replicationSpecs", []):
        for region in spec.get("regionConfigs", []):
            if "electableSpecs" in region:
                region["electableSpecs"]["instanceSize"] = new_size
            if "readOnlySpecs" in region:
                region["readOnlySpecs"]["instanceSize"] = new_size
            if "analyticsSpecs" in region:
                region["analyticsSpecs"]["instanceSize"] = new_size

    payload["replicationSpecs"] = current_config["replicationSpecs"]

    response = session.patch(url, json=payload)
    response.raise_for_status()
    print(f"Cluster size update initiated: {new_size}")


def get_instance_size(config):
    # Navigate through the config to find the instance size
    for spec in config.get("replicationSpecs", []):
        for config in spec.get("regionConfigs", []):
            if "electableSpecs" in config:
                return config["electableSpecs"]["instanceSize"]
    return None


def wait_for_cluster_update():
    while True:
        config = get_current_cluster_config()
        if config["stateName"] == "IDLE":
            print("Cluster update completed")
            break
        print("Waiting for cluster update to complete...")
        time.sleep(60)


def main():
    while True:
        try:
            current_config = get_current_cluster_config()
            current_size = get_instance_size(current_config)

            if current_size is None:
                print("Unable to determine current instance size")
                time.sleep(300)  # Wait for 5 minutes before trying again
                continue

            # Toggle between M10 and M20
            new_size = "R60" if current_size == "R40" else "R40"

            print(f"Current size: {current_size}")
            print(f"Scaling to: {new_size}")

            update_cluster_size(new_size)
            wait_for_cluster_update()

            #print("Waiting for 1 minutes before next scaling operation...")
            #time.sleep(300)  # Wait for 5 minutes
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Waiting for 5 minutes before retrying...")
            time.sleep(300)  # Wait for 5 minutes before retrying


if __name__ == "__main__":
    main()