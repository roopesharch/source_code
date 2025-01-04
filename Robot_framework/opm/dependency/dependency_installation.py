import os
import subprocess
from pathlib import Path
import zipfile

def is_java_installed():
    """
    Check if Java is installed and return its path if found.
    """
    try:
        # Run `java -version` command and check output
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("Java is already installed.")
            return True
    except FileNotFoundError:
        print("Java is not installed.")
    return False

def extract_jdk(zip_path, target_dir):
    """
    Extracts the JDK zip file to the specified directory.
    """
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)
    print(f"JDK extracted to {target_dir}")

def set_java_home(jdk_path):
    """
    Sets JAVA_HOME and updates the PATH environment variable.
    """
    java_home = os.path.abspath(jdk_path)
    # Set JAVA_HOME for the current user
    subprocess.run(["setx", "JAVA_HOME", java_home], check=True)
    
    # Update PATH environment variable
    current_path = os.environ.get("PATH", "")
    if f"{java_home}\\bin" not in current_path:
        new_path = f"{java_home}\\bin;{current_path}"
        subprocess.run(["setx", "/M", "PATH", new_path], check=True)
        print(f"Updated PATH to include {java_home}\\bin")
    else:
        print("PATH already contains JAVA_HOME\\bin")

    print(f"JAVA_HOME set to {java_home}")
    
def is_node_installed():
    """
    Check if Node.js is installed and return its version if found.
    """
    try:
        # Run `node -v` command and check output
        result = subprocess.run(["node", "-v"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Node.js is already installed. Version: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        print("Node.js is not installed.")
    return False

def extract_node(zip_path, target_dir):
    """
    Extracts the Node.js zip file to the specified directory.
    """
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)
    print(f"Node.js extracted to {target_dir}")

def set_node_home(node_path):
    """
    Sets NODE_HOME and updates the PATH environment variable.
    """
    node_home = os.path.abspath(node_path)
    # Set NODE_HOME for the current user
    subprocess.run(["setx", "NODE_HOME", node_home], check=True)
    
    # Update PATH environment variable
    current_path = os.environ.get("PATH", "")
    if f"{node_home}\\bin" not in current_path:
        new_path = f"{node_home};{current_path}"
        subprocess.run(["setx", "/M", "PATH", new_path], check=True)
        print(f"Updated PATH to include {node_home}\\bin")
    else:
        print("PATH already contains NODE_HOME\\bin")

    print(f"NODE_HOME set to {node_home}")