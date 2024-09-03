from datetime import datetime

from airflow.decorators import dag, task

from time import sleep

# from include.helper_functions import expensive_api_call


def expensive_api_call():
    """
    Returns the answer to the question "What is the meaning of life, the universe, and everything?"
    """
    sleep(40)  # sleeping for 100 seconds simulates an expensive API call
    return 42


@dag(start_date=datetime(2023, 1, 1), max_active_runs=3, schedule=None, catchup=False)
def bad_practices_dag_1():

    the_meaning_of_life_the_universe_and_everything = expensive_api_call()

    @task
    def reveal_the_meaning_of_life_the_universe_and_everything(the_answer):
        print(f"The meaning of life, the universe, and everything is... {the_answer}.")

    reveal_the_meaning_of_life_the_universe_and_everything(
        the_meaning_of_life_the_universe_and_everything
    )


bad_practices_dag_1()
