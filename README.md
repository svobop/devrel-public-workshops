# DAG Writing Best Practices Exercises

## Getting started
Refer to the instructions below for each exercise. All DAGs can run locally and on Astro without connecting to external systems. Possible solutions for DAG-related exercises can be found in the `dags/solutions` folder of the repo, although for some exercises there are multiple ways to implement.

Consider using [Ask Astronomer](ask.astronomer.io) if you need additional guidance with any of the exercises.

### Setup
1. Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
2. If you can’t install the CLI, you can run the project from your forked repo using GitHub codespaces.
3. Copy the contents of `.env_example` into a new file called `.env` in the root directory of your project.
4. Start Airflow locally by running `astro dev start`

## DAG authoring exercises
These exercises are designed to get you familiar with commonly used Airflow features and methods for authoring DAGs. Making use of these features will ensure your DAGs are scalable, reliable, and follow best practices. Feel free to use the following resources:

- [Datasets and Data-Aware Scheduling in Airflow guide](https://www.astronomer.io/guides/airflow-datasets/)
- [Dynamic task mapping guide](https://www.astronomer.io/docs/learn/dynamic-tasks)

For exercises in this section, there are three DAGs you will start with from the `dag/exercises/` folder. `Upstream_dag_1` retrieves weather data for a list of cities. `Upstream_dag_2` retrieves sunrise and sunset data for a list of cities. And `downstream_dag` creates a report based on data generated from the two upstream DAGs. 

### Exercise 1: Use Datasets
With Datasets, DAGs that access the same data can have explicit, visible relationships, and DAGs can be scheduled based on updates to these datasets. This feature helps make Airflow data-aware and expands Airflow scheduling capabilities beyond time-based methods such as cron.

The upstream and downstream DAGs in the exercises/ folder are dependent: the upstream DAGs must retrieve the data before the downstream DAG can generate the report. Datasets are the easiest way to implement this dependency.

**Task**: Define a schedule for the downstream DAG to run both every day at midnight UTC AND whenever the Dataset("weather_data") AND Dataset("population_info") are updated.

To implement this, you will also need to modify the `upstream_dag_2` to create the “population_info” dataset. See the DAG code for more hints.

### Exercise 2: Dynamic Task Mapping
With dynamic task mapping, you can write DAGs that dynamically generate parallel tasks at runtime.

**Task**:  Upstream_dag_1 is currently set up to retrieve weather data from just one city. Modify the get_cities task to return all cities in the list, and dynamically map the get_lat_long_for_one_city task over all cities in the list.

### Exercise 3: Default arguments
Best practice is to give your pipelines sensible defaults for your team, to handle task failures and make them more discoverable.

**Task**:  In the downstream DAG, define the default arguments and max consecutive DAG runs. Give it an owner and set the number of retries.

### Exercise 4: XYZ




## Day 2 DAG Operations exercises
Now that DAGs are written, we'll cover deploying DAGs and some of the Day 2 operations that Astro enables.

Feel free to use the following resources:

- [Deploy code to Astro](https://www.astronomer.io/docs/astro/deploy-code)
- [Astro alerts](https://docs.astronomer.io/astro/alerts)
- [Test your Astro project](https://www.astronomer.io/docs/astro/cli/test-your-astro-project-locally)

## Exercise 5: Start a trial and create a Deployment

Start an Astro trial by going to the link provided in the workshop. You can choose a template project to deploy if you wish, or you can skip this step.

Once in Astro, create a new Airflow Deployment. Give it a name, and use the default settings.

## Exercise 6: Deploy code to Astro 

Now that you have a Deployment, you can deploy the code we just worked on. You have two options for this workshop:

1. Deploy using the Astro CLI by running `astro login` to sign in to your trial, and then `astro deploy`.
2. Connect your Astro workspace to your GitHub account and deploy by pushing the code to your fork of the repo. If you don't have the CLI installed, you will need to use this option. Refer to the documentation linked above for more instructions.


## Exercise 7: Create an Astro alert
Astro alerts provide an additional layer of observability over Airflow's built-in alerting system. In the exercises/ folder, one of the DAGs helps highlight this functionality. upstream_dag_1 is parameterized to run with user input. You can simulate a failure of the API that data is retrieved from, or a time delay in a task completing.

**Task**: Set up two alerts in your Astro deployment: a DAG failure alert for the Solution upstream DAG 1, and a task duration alert for the simulate_task_delay task in Solution upstream DAG 1 (try 1 minute). For both alerts, choose email as the communication channel. Try out the alerts by running the Solution upstream DAG 1 with the `Simulate API failure` param set to `True` and the `simulate_task_delay` param set to `120` seconds.

## Exercise 8: Write a DAG validation test
The Astro CLI includes commands that you can use to test and debug DAGs both inside and outside of a locally running Airflow environment. Tests can then be set up to automatically run as part of a CI/CD workflow. Implementing DAG validation tests help you ensure that any new DAG code adheres to your organization’s standards and won’t cause issues in production.

**Task**: Update the appropriate test in the test directory of your Astro project to check that all DAGs have at least `three` retries by default. Run the test using the Astro CLI `astro dev pytest` command. See what happens if you run the test when retries are not set for one of the DAGs.
