from sys import argv
from dagon import Workflow
from dagon.task import TaskType, DagonTask
import os

if __name__ == '__main__':

    if len(argv) != 2:
        print(f"Usage: python {argv[0]} <output directory>")
        exit(1)
        
    output_dir = argv[1]
    print(f"Writting in {output_dir}")

    #### Get Hercules enviroment variables.
    ### NOTE: to use Hercules first export this variables.
    ### HERCULES_CONF is given by the deployment script.
    ### HERCULES_PATH is the root directory where Hercules is installed.
    h_conf = os.getenv("HERCULES_CONF")
    h_path = os.getenv("HERCULES_PATH")
    
    # Format the path to the interception library of Hercules.
    h_preload = None
    if h_path is not None:
        h_preload = "LD_PRELOAD=" + h_path + "build/tools/libhercules_posix.so"

    ### Set enviroment variables required by DagOn.
    if h_conf is not None:
        os.environ['MOUNT_POINT_CONF'] = "HERCULES_CONF=" + h_conf
        print(f"MOUNT_POINT_CONF={h_conf}")
    
    if h_preload is not None:
        os.environ['MOUNT_POINT_LPATH'] = h_preload
        print(f"MOUNT_POINT_LPATH={h_preload}")

    # TODO: here can be defined SSH_WRAPPER as enviroment variable used for scp integration with Hercules.
    # if h_ssh_wrapper is not None:
    #     os.environ['SSH_WRAPPER'] = h_preload

    # Initialize orchestration workflow
    workflow = Workflow("ECG_Distributed_Pipeline")

    # Define variables locally to keep command strings clean
    init = "1"
    end = "2"
    base_dir = "~/software/ECG_Workflow_NodeNFS/pipeline-ecg"
    input_dir = "~/data/ecg_tests/"
    #output_dir = "/home/j.dsanchez/software/dagonstar/examples/ecg/output/"
    # output_dir = "/mnt/hercules/"
    req_dir = f"{base_dir}/requirements"

    # Define Commands
    cmd_stage1 = f"python3 {base_dir}/pipeline-stage1-extraction.py {init} {end} {input_dir} {output_dir} {req_dir}"
    
    cmd_stage2 = f"python3 {base_dir}/pipeline-stage2-processing.py {init} {end} {output_dir} # workflow:///Stage1_Extraction"
    
    # cmd_stage3 = f"python3 {base_dir}/pipeline-stage3-checker.py {init} {end} {output_dir} # workflow:///Stage2_Processing/"
    
    cmd_stage4 = f"python3 {base_dir}/pipeline-stage4-inference.py {init} {end} {output_dir} {req_dir} # workflow:///Stage2_Processing"

    # Create Tasks
    task_stg1 = DagonTask(TaskType.BATCH, "Stage1_Extraction", cmd_stage1)
    task_stg2 = DagonTask(TaskType.BATCH, "Stage2_Processing", cmd_stage2)
    # task_stg3 = DagonTask(TaskType.BATCH, "Stage3_Verification", cmd_stage3)
    task_stg4 = DagonTask(TaskType.BATCH, "Stage4_Inference", cmd_stage4)

    # Add Tasks to workflow
    workflow.add_task(task_stg1)
    workflow.add_task(task_stg2)
    # workflow.add_task(task_stg3)
    workflow.add_task(task_stg4)

    # Automatically generate the execution graph based on the workflow:/// URIs
    workflow.make_dependencies()

    # Run workflow
    workflow.run()