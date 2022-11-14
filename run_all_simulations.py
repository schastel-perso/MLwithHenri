#!/usr/bin/env python3
import random
import subprocess
import hashlib
from multiprocessing import Pool
import sys
import json
import os
import copy

from filelock import FileLock

simulator_input_template = {
    "workflow": {
        "file": "./workflows/1000genome-chameleon-8ch-250k-001.json",
        "reference_flops": "100Mf"
    },
    "error_computation_scheme": "makespan",
    "error_computation_scheme_parameters": {
        "makespan": {
        }
    },
    "scheduling_overhead": "10ms",
    "compute_service_scheme": "all_bare_metal",
    "compute_service_scheme_parameters": {
        "all_bare_metal": {
            "submit_host": {
                "num_cores": "40",
                "speed": "123Gf"
            },
            "slurm_head_host": {
                "num_cores": "40",
                "speed": "423Gf"
            },
            "compute_hosts": {
                "num_hosts": "4",
                "num_cores": "40",
                "speed": "423Gf"
            },
            "properties": {
                "BareMetalComputeServiceProperty::THREAD_STARTUP_OVERHEAD": "42s"
            },
            "payloads": {
            }
        },
    },
    "storage_service_scheme": "submit_only",
    "storage_service_scheme_parameters": {
        "submit_only": {
            "bandwidth_submit_disk_read": "100MBps",
            "bandwidth_submit_disk_write": "10MBps",
            "submit_properties": {
                "StorageServiceProperty::BUFFER_SIZE": "42MB",
                "SimpleStorageServiceProperty::MAX_NUM_CONCURRENT_DATA_CONNECTIONS": "8"
            },
            "submit_payloads": {
            }
        },
    },
    "network_topology_scheme": "many_links",
    "network_topology_scheme_parameters": {
        "many_links": {
            "bandwidth_submit_to_slurm_head": "4MBps",
            "latency_submit_to_slurm_head": "0us",
            "bandwidth_slurm_head_to_compute_hosts": "4MBps",
            "latency_slurm_head_to_compute_hosts": "0us"
        }
    }
}


def run_simulation(args):
    docker_container_id, json_object = args

    sys.stderr.write(".")
    sys.stderr.flush()
    # write json_object to a /tmp/file
    hashcode = hashlib.md5(json.dumps(json_object).encode("utf-8")).hexdigest()
    tmp_file_path = "./simulation_input_" + str(hashcode) + ".json"
    with open(tmp_file_path, "w") as outfile:
        outfile.write(json.dumps(json_object) + "\n")

    # Run the simulator in the container
    command_to_run = "docker exec -it " + docker_container_id + " workflow-simulator-for-calibration " + tmp_file_path

    output = subprocess.check_output(command_to_run, shell=True,
                                     stderr=subprocess.DEVNULL).decode("utf-8")

    # Remove tmp file
    os.remove(tmp_file_path)

    # Produce Output
    makespan = output.split(":")[0]

    # create output object
    output_object = {
        "makespan": makespan,
        "workflow": json_object["workflow"]["file"],
        "scheduling_overhead": json_object["scheduling_overhead"],
        "submit_host_num_cores": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["submit_host"][
            "num_cores"],
        "submit_host_speed": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["submit_host"]["speed"],
        "slurm_head_host_num_cores":
            json_object["compute_service_scheme_parameters"]["all_bare_metal"]["slurm_head_host"]["num_cores"],
        "slurm_head_host_speed": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["slurm_head_host"][
            "speed"],
        "compute_hosts_num_hosts": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"][
            "num_hosts"],
        "compute_hosts_num_cores": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"][
            "num_cores"],
        "compute_hosts_speed": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"][
            "speed"],
        "thread_startup_overhead": json_object["compute_service_scheme_parameters"]["all_bare_metal"]["properties"][
            "BareMetalComputeServiceProperty::THREAD_STARTUP_OVERHEAD"],
        "submit_disk_read_bandwidth": json_object["storage_service_scheme_parameters"]["submit_only"][
            "bandwidth_submit_disk_read"],
        "submit_disk_write_bandwidth": json_object["storage_service_scheme_parameters"]["submit_only"][
            "bandwidth_submit_disk_write"],
        "submit_storage_buffer_size":
            json_object["storage_service_scheme_parameters"]["submit_only"]["submit_properties"][
                "StorageServiceProperty::BUFFER_SIZE"],
        "submit_storage_num_connections":
            json_object["storage_service_scheme_parameters"]["submit_only"]["submit_properties"][
                "SimpleStorageServiceProperty::MAX_NUM_CONCURRENT_DATA_CONNECTIONS"],
        "bandwidth_submit_to_slurm_head": json_object["network_topology_scheme_parameters"]["many_links"][
            "bandwidth_submit_to_slurm_head"],
        "bandwidth_slurm_head_to_compute_hosts": json_object["network_topology_scheme_parameters"]["many_links"][
            "bandwidth_slurm_head_to_compute_hosts"]
    }

    with FileLock("./data.txt.lock"):
        with open("./data.txt", "a") as outfile:
            outfile.write(json.dumps(output_object) + "\n")

    return


def main():
    try:
        num_samples = int(sys.argv[1])
        num_threads = int(sys.argv[2])
    except Exception:
        sys.stderr.write("Usage: " + sys.argv[0] + " <# xps> <# threads>\n")
        sys.exit(1)

    # Start Docker container
    ##############################
    docker_command = "docker run -it -d -v " + os.getcwd() + ":/home/me serge"
    docker_container_id = subprocess.check_output(docker_command, shell=True).decode("utf-8").rstrip()

    # Hard-coded - each workflow is different
    workflow = "./workflows/srasearch-chameleon-10a-003.json"

    overhead_min = 0
    overhead_max = 10

    num_cores_min = 1
    num_cores_max = 32

    num_hosts_min = 1
    num_hosts_max = 64

    core_speed_min = 1 * 1000 * 1000 * 1000
    core_speed_max = 1000 * 1000 * 1000 * 1000

    disk_bandwidth_min = 1 * 1000 * 1000
    disk_bandwidth_max = 100 * 1000 * 1000

    buffer_size_min = 1 * 1000 * 1000
    buffer_size_max = 100 * 1000 * 1000

    num_concurrent_connections_min = 1
    num_concurrent_connections_max = 10

    network_bandwidth_min = 1 * 1000 * 1000
    network_bandwidth_max = 100 * 1000 * 1000

    stuff_to_run = []

    for s in range(0, num_samples):
        simulator_input = copy.deepcopy(simulator_input_template)

        simulator_input["workflow"]["file"] = workflow

        simulator_input["scheduling_overhead"] = str(random.uniform(overhead_min, overhead_max))

        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["submit_host"]["num_cores"] = str(
            random.randint(num_cores_min, num_cores_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["submit_host"]["speed"] = str(
            random.randint(core_speed_min, core_speed_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["slurm_head_host"]["num_cores"] = str(
            random.randint(num_cores_min, num_cores_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["slurm_head_host"]["speed"] = str(
            random.randint(core_speed_min, core_speed_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"]["num_hosts"] = str(
            random.randint(num_hosts_min, num_hosts_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"]["num_cores"] = str(
            random.randint(num_cores_min, num_cores_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["compute_hosts"]["speed"] = str(
            random.randint(core_speed_min, core_speed_max))
        simulator_input["compute_service_scheme_parameters"]["all_bare_metal"]["properties"][
            "BareMetalComputeServiceProperty::THREAD_STARTUP_OVERHEAD"] = str(
            random.uniform(overhead_min, overhead_max))

        simulator_input["storage_service_scheme_parameters"]["submit_only"]["bandwidth_submit_disk_read"] = str(
            random.randint(disk_bandwidth_min, disk_bandwidth_max))
        simulator_input["storage_service_scheme_parameters"]["submit_only"]["bandwidth_submit_disk_write"] = str(
            random.randint(disk_bandwidth_min, disk_bandwidth_max))
        simulator_input["storage_service_scheme_parameters"]["submit_only"]["submit_properties"][
            "StorageServiceProperty::BUFFER_SIZE"] = str(random.randint(buffer_size_min, buffer_size_max))
        simulator_input["storage_service_scheme_parameters"]["submit_only"]["submit_properties"][
            "SimpleStorageServiceProperty::MAX_NUM_CONCURRENT_DATA_CONNECTIONS"] = str(
            random.randint(num_concurrent_connections_min, num_concurrent_connections_max))

        simulator_input["network_topology_scheme_parameters"]["many_links"]["bandwidth_submit_to_slurm_head"] = str(
            random.randint(network_bandwidth_min, network_bandwidth_max))
        simulator_input["network_topology_scheme_parameters"]["many_links"][
            "bandwidth_slurm_head_to_compute_hosts"] = str(random.randint(network_bandwidth_min, network_bandwidth_max))

        stuff_to_run.append((docker_container_id, simulator_input.copy()))

    # Run everything
    ###############################
    sys.stderr.write("Running " + str(len(stuff_to_run)) + " experiments with " + str(num_threads) +
                     " threads. Results written to ./data.txt")
    with Pool(num_threads) as p:
        p.map(run_simulation, stuff_to_run)

    # Clean up
    #############################
    docker_command = "docker kill " + docker_container_id
    os.system(docker_command)
    sys.stderr.write("\n")  # avoid weird terminal stuff?
    # os.system("reset")


if __name__ == "__main__":
    main()
