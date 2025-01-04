from robot import run
from robot.rebot import rebot
import os
import shutil
import json
import uuid
import glob
import threading
import sys
from datetime import datetime
from core.mail import send_email
from core.setup import read_conf, read_json
from dependency.dependency_installation import *
import warnings
warnings.filterwarnings('ignore')

if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS.replace('\\', '/')
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')

# Get the Documents folder path
documents_path = os.path.join(os.path.expanduser("~"), "Documents/OPM-AUTOVALIDATION").replace('\\', '/')
dependency_installation_path = "C:/Program Files"
# Directory to store individual output files
output_dir = "outputs"
# Path to the JDK zip file
jdk_zip = "dependency/jdk-22.zip"
# Target directory to extract the JDK
java_target_dir = documents_path + "/dependency/Java"
# Path to the Node.js zip file
node_zip = "dependency/nodejs.zip"  # Replace with the path to your Node.js zip
# Target directory to extract Node.js
node_target_dir = documents_path + "/dependency/NodeJS"

def auto_exit():
    print("\nExiting program automatically due to timeout...\n")
    os._exit(0)
    
if __name__ == "__main__":
    # Reading config json
    data = read_json('UIAutomation.json')
    
    try:
        if is_java_installed():
            print("\nSkipping JDK setup.\n")
        else:
            print("Java not found. Setting up JDK...\n")
            extract_jdk(jdk_zip, java_target_dir)
            set_java_home(java_target_dir + "/jdk-22")
            print("JDK setup completed successfully.\n")
                
        if is_node_installed():
            print("\nSkipping Node.js setup.\n")
        else:
            print("Node.js not found. Setting up Node.js...\n")
            extract_node(node_zip, node_target_dir)
            set_node_home(node_target_dir + "/nodejs")
            print("Node.js setup completed successfully.\n")
    except Exception as e:
        print(f"An error occurred: {e}")
        
    now = datetime.now()
    now = str(now).replace(":", "-")[:16]
    reportName = (data['reportName'] if 'reportName' in data else str(uuid.uuid4())) + " - " + now
    file_path = f"/score_report/{reportName}.xlsx"

    try:
        failed_kpis = []  # Initialize the list for failed KPIs
        max_retries = int(read_conf('configurations', 'max_retry_attempts'))   # Read Mmximum number of retry attempts from config

        # Main loop to run the scripts for each KPI
        for i, config in enumerate(data['kpis']):
            # ignore KPI if autovalidate in JSON is False
            if config['auto_validate'].lower() == 'false':  continue

            # create temp JSON to access from dependency python files
            json_object = json.dumps(config, indent=4)
            # Writing to temp.json
            with open("temp.json", "w") as outfile:
                outfile.write(json_object)

            output_file = os.path.join(documents_path, output_dir, reportName, f"output_{i + 1}.xml")
            report_file = os.path.join(documents_path, output_dir, reportName, f"report_{i + 1}.html")
            log_file = os.path.join(documents_path, output_dir, reportName, f"log_{i + 1}.html")

            # Prepare variables for Robot Framework
            variables = [
                f"config:{config}",
                f"brands:{data['brands']}",
                f"tiers:{data['tiers']}",
                f"filePath:{file_path}",
                f"sheetName:{config.get('sheetName', 'sheet' + str(i + 1))}",
                f"SUITE_NAME:{config['scorecardName']}"
            ]
            
            # Run the Robot script
            result = run(
                "core/DATA_VALIDATION.robot",
                name=f"{' - '.join([config['scorecardName'], config['kpiName'], config['filters']['date']])}",
                variable=variables,
                output=output_file,
                report=report_file,
                log=log_file
            )
            
            # If it fails, add to the retry list
            if result != 0:
                failed_kpis.append({
                    "name": f"{' - '.join([config['scorecardName'], config['kpiName'], config['filters']['date']])}", 
                    "variables": variables, 
                    "output": output_file,
                    "report": report_file,
                    "log": log_file,
                    "retryCount": max_retries
                })
        
        if failed_kpis and max_retries > 0:
            print('\nRetrying the failed KPIs...\n')
            
            # Retry logic for failed KPIs
            while failed_kpis:
                kpi_data = failed_kpis.pop(0)  # Remove the first entry to avoid infinite loop
                
                if kpi_data['retryCount'] != 0:
                    print(f"\nRerunning the failed KPI: {kpi_data['name']}\nRetry Attempt Count: {kpi_data['retryCount']}\n")
                    
                    result = run(
                        "core/DATA_VALIDATION.robot",
                        name=kpi_data['name'], 
                        variable=kpi_data['variables'], 
                        output=kpi_data['output'],
                        report=kpi_data['report'],
                        log=kpi_data['log']
                    )
                    
                    # If it still fails and retries are remaining
                    if result != 0 and kpi_data['retryCount'] > 1:
                        kpi_data['retryCount'] -= 1
                        failed_kpis.append(kpi_data)  # Re-add to the list for another attempt

    except Exception as e:
        raise Exception(f"Error running the robot scripts: {e}")        
        
    # Merge all output files into one report
    output_files = [os.path.join(documents_path, output_dir, reportName, f"output_{i}.xml") for i in range(1, len(data['kpis']) + 1) if data['kpis'][i-1]['auto_validate'].lower() != 'false']
    final_output = os.path.join(documents_path, output_dir, reportName, f"final_output - {now}.xml")
    final_report = os.path.join(documents_path, output_dir, reportName, f"final_report - {now}.html")
    final_log = os.path.join(documents_path, output_dir, reportName, f"final_log - {now}.html")

    # Move failure screenshots to output file
    screenshot_file_list = glob.glob(BASE_DIR + "/browser/screenshot/*.png")
    destination_path = documents_path + "/outputs/" + reportName + '/failed_element_screenshot'

    file_count = 0
    for i in screenshot_file_list:
        shutil.move(i, destination_path + str(file_count) + '.png')
        file_count += 1

    # Use rebot to merge the results
    print("\nGenerating final report...\n")
    rebot(*output_files, name=reportName, output=final_output, report=final_report, log=final_log)

    # Send the final report as a mail if turn_email_off boolean set to True in JSON
    if data['turn_email_off'] == "True":
        send_email(f"QA Auto Validation Report - {now}", data['recipientMailAddress'],
                   f"Hi Team,\n\nPlease find attached the report of the autovalidation that occurred on {now}.\n\nThis is an automated email. Please do not reply.\n\nRegards,\nAuto Validation Team",
                   [documents_path + file_path])
    else:
        print("Ignoring report Emailing based on turn_email_off key in JSON")
    print(f"Exported excel: \033[4m{(documents_path + file_path).replace('/', '\\')}\033[0m\n")

    search_pattern = os.path.join(documents_path, f"playwright*")
    # Find all files matching the prefix
    matching_files = glob.glob(search_pattern)

    # Delete playwright log files
    for file_path in matching_files:
        try:
            # Check if it's a file (not a directory)
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            # print(f"Error deleting {file_path}: {e}")
            continue

    # Auto close the application exe after 60s.
    print("QA Auto Validation Completed.\n")
    threading.Timer(60, auto_exit).start()
    try:
        input("Press Enter to exit manually before 60 seconds...\n")
        print("Exiting program manually...")
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting...")
