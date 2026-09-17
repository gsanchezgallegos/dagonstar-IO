import os
import subprocess
from sys import argv, exit
from dagon import Workflow
from dagon.task import DagonTask, TaskType

if __name__ == "__main__":

    if len(argv) != 4:
        print(f"Usage: python {argv[0]} <output_directory> <total_end> <chunk_size>")
        exit(1)

    output_dir = argv[1]
    # Define variables locally to keep command strings clean
    total_init = 1
    try:
        total_end = int(argv[2])
        chunk_size = int(argv[3])
        if total_end < 1 or chunk_size < 1:
            raise ValueError("Values for 'total_end' and 'chunk_size' must be positive integers.")
    except ValueError as e:
        print(f"Argument Error: {e}")
        exit(1)

    print(f"Writting in {output_dir}")

    #### Get Hercules enviroment variables.
    ### NOTE: to use with Hercules first export this variables.
    ### HERCULES_CONF is given by the deployment script.
    ### HERCULES_PATH is the root directory where Hercules is installed.
    h_conf = os.getenv("HERCULES_CONF")
    h_path = os.getenv("HERCULES_PATH")

    # Format the path to the interception library of Hercules.
    h_preload = None
    if h_path is not None:
        h_preload = (
            "LD_PRELOAD=" + os.path.join(h_path, "build/tools/libhercules_posix.so")
        )

    ### Set enviroment variables required by DagOn.
    if h_conf is not None:
        os.environ["MOUNT_POINT_CONF"] = "HERCULES_CONF=" + h_conf
        print(f"MOUNT_POINT_CONF={h_conf}")

    if h_preload is not None:
        os.environ["MOUNT_POINT_LPATH"] = h_preload
        print(f"MOUNT_POINT_LPATH={h_preload}")

    # directory used for IO operations.
    os.environ["MOUNT_POINT"] = output_dir

    # TODO: here can be defined SSH_WRAPPER as enviroment variable used for scp integration with Hercules.
    # if h_ssh_wrapper is not None:
    #     os.environ['SSH_WRAPPER'] = h_preload

    # Initialize orchestration workflow
    workflow = Workflow("ECG_Distributed_Pipeline")

    # total_end = 10
    # chunk_size = 10

    # TODO: CHANGE THIS PATH
    base_dir = "~/software/ECG_Workflow_NodeNFS/pipeline-ecg"
    # TODO: CHANGE THIS PATH
    input_dir = "/lustre/uc3m_a0/dynamic/gesanche/data/ecg_tests/"
    # output_dir = "/home/j.dsanchez/software/dagonstar/examples/ecg/output/"
    # output_dir = "/mnt/hercules/"
    req_dir = f"{base_dir}/requirements"

    # Define cluster nodes for computational load distribution.
    # Determine the exact list of nodes reserved by Slurm (SLURM_JOB_NODELIST / SLURM_NODELIST),
    # falling back to HERCULES_MPI_HOSTFILE_NAME when running outside of Slurm.
    slurm_nodelist = os.getenv("SLURM_JOB_NODELIST") or os.getenv("SLURM_NODELIST")
    lines = []

    if slurm_nodelist:
        # Resolve Slurm nodelist to individual hostnames using scontrol
        try:
            res = subprocess.run(
                ["scontrol", "show", "hostnames", slurm_nodelist],
                capture_output=True, text=True, check=True
            )
            lines = [h.strip() for h in res.stdout.splitlines() if h.strip()]
        except Exception:
            pass

    # Fallback to HERCULES_MPI_HOSTFILE_NAME if outside Slurm or nodelist not detected
    if not lines:
        hostfile_path = os.getenv("HERCULES_MPI_HOSTFILE_NAME")
        if hostfile_path and os.path.exists(hostfile_path):
            try:
                with open(hostfile_path, 'r') as f:
                    lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            except IOError as e:
                print(f"I/O Exception reading hostfile: {e}")
                exit(1)

    # Restrict to SLURM_NNODES if set
    slurm_nnodes = os.getenv("SLURM_NNODES") or os.getenv("SLURM_JOB_NUM_NODES")
    if slurm_nnodes and len(lines) > int(slurm_nnodes):
        try:
            lines = lines[:int(slurm_nnodes)]
        except ValueError:
            pass

    if not lines:
        print("Error: Could not determine reserved nodes from Slurm (SLURM_JOB_NODELIST) or HERCULES_MPI_HOSTFILE_NAME.")
        exit(1)

    # Fill cluster_nodes with repeated nodes if there are not enough lines
    cluster_nodes = []
    num_chunks = len(range(total_init, total_end + 1, chunk_size))
    total_nodes = max(len(lines), num_chunks)
    for i in range(total_nodes):
        node_ip = lines[i % len(lines)]
        cluster_nodes.append({"ip": node_ip, "user": "gesanche"})

    print(f"Detected Nodes: {cluster_nodes}")
    num_nodes = len(cluster_nodes)

    # Construct chunked tasks to execute workflow streams in parallel
    for idx, c_start in enumerate(range(total_init, total_end + 1, chunk_size)):
        c_end = min(c_start + chunk_size - 1, total_end)
        chunk_suffix = f"{c_start}-{c_end}"

        # Allocate contiguous chunks to nodes via round-robin deterministic assignment
        target_node = cluster_nodes[idx % num_nodes]
        node_ip = target_node["ip"]
        node_user = target_node["user"]

        stg1_name = f"Stage1_Extraction_{chunk_suffix}"
        stg2_name = f"Stage2_Processing_{chunk_suffix}"
        stg4_name = f"Stage4_Inference_{chunk_suffix}"

        # Define Commands
        cmd_stage1 = f". ~/software/dagonstar-IO/.venv/bin/activate; python3 {base_dir}/pipeline-stage1-extraction.py {c_start} {c_end} {input_dir} {output_dir} {req_dir}"
        cmd_stage2 = f". ~/software/dagonstar-IO/.venv/bin/activate; python3 {base_dir}/pipeline-stage2-processing.py {c_start} {c_end} {output_dir} # workflow:///{stg1_name}"
        cmd_stage4 = f". ~/software/dagonstar-IO/.venv/bin/activate; python3 {base_dir}/pipeline-stage4-inference.py {c_start} {c_end} {output_dir} {req_dir} # workflow:///{stg2_name}"

        # Create Tasks with SSH context variables evaluated
        task_stg1 = DagonTask(TaskType.BATCH, stg1_name, cmd_stage1, ip=node_ip, ssh_username=node_user)
        task_stg2 = DagonTask(TaskType.BATCH, stg2_name, cmd_stage2, ip=node_ip, ssh_username=node_user)
        task_stg4 = DagonTask(TaskType.BATCH, stg4_name, cmd_stage4, ip=node_ip, ssh_username=node_user)

        # Add Tasks to workflow
        workflow.add_task(task_stg1)
        workflow.add_task(task_stg2)
        # workflow.add_task(task_stg3)
        workflow.add_task(task_stg4)

    # Automatically generate the execution graph based on the workflow:/// URIs
    workflow.make_dependencies()

    # Run workflow
    workflow.run()