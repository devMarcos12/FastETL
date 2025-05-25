## FastETL

A simple ETL agent that automates the extraction, transformation, and loading of supermarket data.

## Description

FastETL is designed to simplify the ETL (Extract, Transform, Load) process for supermarket data, making automation of these steps straightforward and efficient. The project is written entirely in Python and uses uv for dependency management, ensuring reproducible environments and fast installations.

## Installation

First, create and activate a virtual environment (recommended):

On Linux/macOS:

  ```
python3 -m venv .venv
source .venv/bin/activate
  ```

On Windows:
  ```
python -m venv .venv
.venv\Scripts\activate
  ```

First, install uv (if you don't have it yet):
  ```
pip install uv
  ```

Then, install the project dependencies:

  ```
uv pip install -r pyproject.toml
  ```

## Usage

1. Create the .env file based on .env.example and configure it as needed.

2. First Create the tables on relational Database the
  ```
    python models.py
  ```

3. Populate the tables:
  ```
    python populate.py
  ```

4. Run the main file to start the ETL process:
  ```
    python main.py
  ```
