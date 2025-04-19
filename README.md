# orpheus_engine

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/guides/build/projects/creating-a-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `orpheus_engine/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `orpheus_engine_tests` directory and you can run tests using `pytest`:

```bash
pytest orpheus_engine_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster+

The easiest way to deploy your Dagster project is to use Dagster+.

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/) to learn more.

## Docker Compose Deployment (Postgres + S3)

This project includes a Docker Compose setup for deploying Dagster with PostgreSQL for metadata storage and AWS S3 for IO management. This is suitable for local testing and deployment platforms like Coolify.

**Prerequisites:**

*   Docker and Docker Compose installed.
*   AWS Credentials (Access Key ID, Secret Access Key, Region) configured where your environment can access them.
*   An S3 bucket created for Dagster's IO Manager.
*   PostgreSQL connection details (User, Password, DB Name, Host, Port).
    *   If deploying via Coolify, you can use Coolify's managed PostgreSQL service instead of the one defined in `docker-compose.yml`.

**Configuration (Local Environment):**

1.  **Create `.env` file:** In the project root directory (`orpheus-engine/`), create a file named `.env`. **Do not commit this file to Git.**
2.  **Populate `.env`:** Add the following environment variables to the `.env` file, replacing the placeholder values with your actual credentials and settings:

    ```env
    # --- Dagster & Postgres --- 
    # Use 'orpheus_engine_postgres' as host if using the compose service
    # If using external/Coolify Postgres, use its connection details
    DAGSTER_POSTGRES_USER=dagster
    DAGSTER_POSTGRES_PASSWORD=dagster
    DAGSTER_POSTGRES_DB=dagster
    POSTGRES_HOST=orpheus_engine_postgres
    POSTGRES_PORT=5432
    
    # Optional: Port mapping for Dagster Webserver on your host machine
    DAGSTER_PORT=3000

    # --- AWS --- 
    AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
    AWS_REGION=your-aws-region # e.g., us-east-1
    DAGSTER_S3_BUCKET=your-dagster-io-manager-bucket-name # Bucket for IO Manager

    # --- Application Secrets --- 
    LOOPS_API_KEY=YOUR_LOOPS_API_KEY
    LOOPS_SESSION_TOKEN=YOUR_LOOPS_SESSION_TOKEN # If used
    GOOGLE_MAPS_API_KEY=YOUR_GOOGLE_MAPS_API_KEY
    GENDERIZE_API_KEY=YOUR_GENDERIZE_API_KEY # Optional
    AIRTABLE_PERSONAL_ACCESS_TOKEN=YOUR_AIRTABLE_TOKEN
    OPENAI_API_KEY=YOUR_OPENAI_KEY # Or other LiteLLM keys
    WAREHOUSE_COOLIFY_URL=YOUR_DLT_POSTGRES_URL # e.g., postgresql://user:pass@host:port/db

    # --- Dagster Environment --- 
    # Set to 'development' for local testing (enables caches, etc.) 
    # Set to 'production' for deployed environments
    DAGSTER_ENV=development 
    ```

**Running Locally:**

1.  Navigate to the deployment directory:
    ```bash
    cd deploy/docker
    ```
2.  Build the Docker images:
    ```bash
    docker-compose build
    ```
3.  Start the services in detached mode:
    ```bash
    docker-compose up -d
    ```
4.  Access the Dagster UI in your browser:
    `http://localhost:3000` (or the port set by `DAGSTER_PORT` in your `.env` file).

**Stopping Services:**

```bash
cd deploy/docker
docker-compose down
```

**Coolify Deployment:**

1.  Push your code (including the `deploy/docker` directory) to your Git repository.
2.  In Coolify, create a new Application sourced from your Git repository.
3.  Select "Docker Compose" as the build pack.
4.  Point Coolify to the `deploy/docker/docker-compose.yml` file.
5.  **Database:**
    *   **Option A (Recommended for Coolify): Use Coolify Managed Postgres.**
        *   Ensure the `orpheus_engine_postgres` service and `postgres_data` volume are removed from your `deploy/docker/docker-compose.yml` (as done in the latest commits).
        *   Add a PostgreSQL service resource within your Coolify project.
    *   **Option B: Use Compose Managed Postgres.**
        *   Keep the `orpheus_engine_postgres` service and `postgres_data` volume definitions in `docker-compose.yml`.
6.  **Environment Variables:** Configure *all* the necessary environment variables (as listed in the `.env` section above) within the Coolify application's "Environment Variables" settings. Use Coolify secrets for sensitive values.
    *   **If using Coolify Managed Postgres (Option A):** You *must* set `POSTGRES_HOST`, `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_DB`, and `POSTGRES_PORT` in Coolify to match the connection details provided by the Coolify managed database service.
    *   **Crucially, set `DAGSTER_ENV=production` for deployed environments.**
7.  Deploy the application via Coolify.
