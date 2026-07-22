from dagon import Workflow
from dagon.task import TaskType, DagonTask

if __name__ == '__main__':

    # Initialize orchestration workflow
    workflow = Workflow("ECG_Distributed_Pipeline")

    # Define variables locally to keep command strings clean
    init = "1"
    end = "2"
    base_dir = "/home/j.dsanchez/software/pipeline-ecg"
    input_dir = "/home/j.dsanchez/data/ecg_tests/"
    output_dir = "/mnt/hercules/"
    # output_dir = "/home/j.dsanchez/software/dagonstar/examples/ecg/output/"
    req_dir = f"{base_dir}/requirements"

    # Define Commands (Keep the workflow:/// comments intact for graph generation)
    cmd_stage1 = f". /home/j.dsanchez/software/dagonstar/.venv/bin/activate;  python3 {base_dir}/pipeline-stage1-extraction.py {init} {end} {input_dir} {output_dir} {req_dir}"
    cmd_stage2 = f". /home/j.dsanchez/software/dagonstar/.venv/bin/activate;  python3 {base_dir}/pipeline-stage2-processing.py {init} {end} {output_dir} # workflow:///Stage1_Extraction/"
    cmd_stage4 = f". /home/j.dsanchez/software/dagonstar/.venv/bin/activate;  python3 {base_dir}/pipeline-stage4-inference.py {init} {end} {output_dir} {req_dir} # workflow:///Stage2_Processing/"

    # Define IP and usernames for your 3 target remote machines
    M1_IP, M1_USER = "wn01", "j.dsanchez"
    M2_IP, M2_USER = "wn02", "j.dsanchez"
    M3_IP, M3_USER = "wn03", "j.dsanchez"

    # Create Tasks using TaskType.BATCH with remote ip and ssh_username arguments
    task_stg1 = DagonTask(TaskType.BATCH, "Stage1_Extraction", cmd_stage1, ip=M1_IP, ssh_username=M1_USER)
    task_stg2 = DagonTask(TaskType.BATCH, "Stage2_Processing", cmd_stage2, ip=M2_IP, ssh_username=M2_USER)
    task_stg4 = DagonTask(TaskType.BATCH, "Stage4_Inference", cmd_stage4, ip=M3_IP, ssh_username=M3_USER)

    # Add Tasks to workflow
    workflow.add_task(task_stg1)
    workflow.add_task(task_stg2)
    workflow.add_task(task_stg4)

    # Automatically generate the execution graph based on the workflow:/// URIs
    workflow.make_dependencies()

    # Run workflow
    workflow.run()