# Firmable Data Pipeline Project

## Overview

This repository implements a modular data pipeline to:
- Extract data from two different websites.
- Store multiple CSV files as raw data.
- Load the data into a PostgreSQL database using Python scripts.
- Clean and transform the data using dbt.
- Orchestrate the entire workflow using Apache Airflow.

---

## Architecture

![image](https://github.com/user-attachments/assets/210d147f-d1ae-45ad-ab4a-e6dba23c5c65)


## Repository Structure

```
airflow/            # Airflow DAGs, plugins, and config files
  ├── dags/
  └── plugins/
data/
  ├── raw/          # Raw CSV files
  └── processed/    # (Optional) Processed/intermediate files
dbt/                # dbt project for data cleaning
  ├── models/
  │   ├── staging/
  │   └── marts/
  ├── seeds/
  ├── snapshots/
  ├── dbt_project.yml
  └── profiles.yml
scripts/            # Python scripts for extraction & loading
  ├── extract_website1.py
  ├── extract_website2.py
  ├── load_to_postgres.py
  └── requirements.txt
config/             # Configuration and secrets
  ├── config.yaml
  └── secrets.env
tests/              # Unit/integration tests
  └── test_data_pipeline.py
.gitignore
README.md
LICENSE
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL
- dbt
- Apache Airflow

### Setup Instructions

1. **Clone the repository**
    ```bash
    git clone https://github.com/your-username/firmable.git
    cd firmable
    ```

2. **Install Python dependencies**
    ```bash
    pip install -r scripts/requirements.txt
    ```

3. **Configure Airflow**
    - Place your DAGs in `airflow/dags`
    - Configure `airflow.cfg` as needed

4. **Set up dbt**
    - Edit `dbt/profiles.yml` and `dbt_project.yml` for your environment

5. **Configure connections and secrets**
    - Add your configuration to `config/config.yaml`
    - Store sensitive information in `config/secrets.env` (do not commit this file)

---

## Usage

1. **Extract Data**
    - Run scripts in `scripts/` or trigger via Airflow DAG

2. **Load to PostgreSQL**
    - Use `load_to_postgres.py` or trigger via Airflow

3. **Clean Data with dbt**
    - From the `dbt/` directory, run:
      ```bash
      dbt run
      ```

4. **Orchestrate with Airflow**
    - Start the scheduler and webserver:
      ```bash
      airflow scheduler
      airflow webserver
      ```

---

## License

[MIT License](LICENSE)
