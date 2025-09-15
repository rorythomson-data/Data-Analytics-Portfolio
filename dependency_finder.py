import os
import ast
import importlib.util

# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
PROJECT_DIR = "."  # Root directory of your project
OUTPUT_FILE = "requirements_auto.txt"

# Packages to ignore (development, notebook, or env-related)
EXCLUDED_PACKAGES = {
    "ipython", "jupyter", "jupyterlab", "notebook", "ipywidgets", "ipykernel",
    "setuptools", "pip", "wheel", "debugpy", "traitlets", "tornado",
    "prompt_toolkit", "wcwidth", "pygments", "jupyter_client", "jupyter_core",
    "jupyter_server", "jupyterlab_server", "notebook_shim"
}

# --------------------------------------------
# FUNCTIONS
# --------------------------------------------

def is_builtin_or_stdlib(module_name):
    """
    Check if a module is built-in or part of the standard library.
    """
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            return True  # Likely built-in or cannot be found
        origin = spec.origin or ''
        return 'site-packages' not in origin
    except (ValueError, ModuleNotFoundError):
        return True  # Treat as stdlib if error occurs


def extract_imports_from_file(file_path):
    """
    Extract all top-level imports from a Python file.
    """
    imports = set()
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            node = ast.parse(f.read(), filename=file_path)
            for n in ast.walk(node):
                if isinstance(n, ast.Import):
                    for alias in n.names:
                        imports.add(alias.name.split('.')[0])
                elif isinstance(n, ast.ImportFrom):
                    if n.module:
                        imports.add(n.module.split('.')[0])
    except Exception as e:
        print(f"⚠️ Skipped {file_path} due to parse error: {e}")
    return imports


def scan_project_for_imports(project_dir):
    """
    Recursively scan all .py files in the project directory.
    """
    all_imports = set()
    for root, _, files in os.walk(project_dir):
        # Skip virtual environments and hidden directories
        if 'venv' in root or '.git' in root or '__pycache__' in root:
            continue
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                all_imports.update(extract_imports_from_file(file_path))
    return all_imports


def main():
    print("🔍 Scanning project for imports...")
    all_imports = scan_project_for_imports(PROJECT_DIR)
    third_party_imports = {
        imp for imp in all_imports
        if not is_builtin_or_stdlib(imp) and imp.lower() not in EXCLUDED_PACKAGES
    }

    # Sort alphabetically
    sorted_imports = sorted(third_party_imports)

    # Save to requirements_auto.txt
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for pkg in sorted_imports:
            f.write(f"{pkg}\n")

    print(f"✅ Found {len(sorted_imports)} dependencies.")
    print(f"📦 Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
