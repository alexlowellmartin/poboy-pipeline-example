<p align="center"><img src="https://github.com/alexlowellmartin/poboy-pipeline-example/blob/8ad801d055bd1937c47baef1244b198455b8da11/poboy-pipeline-example-dag.png" width="600" alt="DAG preview image"></p>

# poboy-pipeline-example

## Overview
Building a po' boy's spatial data pipeline from scratch with Dagster, geoparquet, &amp; R2. (This is an example)

## External links
* [Building a po' boy's spatial data pipeline from scratch with Dagster, GeoParquet, & R2](https://alexlowellmartin.com/building-a-po-boys-spatial-data-pipeline-from-scratch-with-dagster-geoparquet-r2-2/)
* [Dagster - Getting Started](https://docs.dagster.io/getting-started)
* [GeoParquet.org](https://geoparquet.org/)
* [Cloudflare R2](https://www.cloudflare.com/developer-platform/r2/)

## Getting started

Visit the Dagster [Getting started](https://docs.dagster.io/getting-started) page. Dagster supports Python 3.8 through 3.12. Ensure you have one of the supported Python versions installed before proceeding.

First, Clone this Dagster repository.

```bash
git clone https://github.com/alexlowellmartin/poboy-pipeline-example && cd poboy-pipeline-example
```

Second, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `poboy_pipeline_example/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.
