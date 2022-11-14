#!/usr/bin/env python3

import random
import subprocess
import hashlib
from multiprocessing import Pool
import sys
import json
import os
import copy


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
    command_to_run = ["/usr/bin/docker",
                      "exec",
                      docker_container_id,
                      "workflow-simulator-for-calibration",
                      tmp_file_path]

    makespan = "NaN"
    with subprocess.Popen(command_to_run, stdout=subprocess.PIPE) as process:
        try:
            out, err = process.communicate()
            # Henri wants strings to represent his floats
            # ... but I want to make sure the output is a valid float
            makespan = "%f" % float(out.decode("utf-8").split(":")[0])
        except ValueError:
            sys.stderr.write("Issue with makespan value ->%s<-" % makespan)
            pass

    # Remove tmp file
    os.remove(tmp_file_path)

    # create output object
    output_object = {
        "makespan": makespan,  # Henri wants strings
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

    return json.dumps(output_object)


def main(parameters):
    outfilename, num_samples, num_threads = parameters
    # Start Docker container
    ##############################
    docker_command = "docker run --rm -it -d -v " + os.getcwd() + ":/home/me serge"
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
                     " threads. Results written to " + outfilename + "\n")
    with Pool(num_threads) as p:
        outputs = p.map(run_simulation, stuff_to_run)

    # Clean up
    #############################
    sys.stderr.write("Stopping and removing docker container " + docker_container_id + " \n")
    docker_command = "docker stop " + docker_container_id
    os.system(docker_command)
    sys.stderr.write("\n")  # avoid weird terminal stuff?

    with open(outfilename, "a") as outfile:
        outfile.write("\n".join(outputs))
        outfile.write("\n")


def process_arguments(progname):
    import argparse
    parser = argparse.ArgumentParser(
                    prog=progname,
                    description='Run simulations',
                    epilog='Text at the bottom of help')
    parser.add_argument('num_samples', type=int)
    parser.add_argument('num_threads', type=int)
    parser.add_argument('-o', '--outfilename', dest='outfilename', default='data.txt')
    args = parser.parse_args()
    return args.outfilename, args.num_samples, args.num_threads


if __name__ == "__main__":
    arguments = process_arguments(sys.argv[0])
    main(arguments)
