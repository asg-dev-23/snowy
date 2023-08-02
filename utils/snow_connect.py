import streamlit as st

from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION
from dotenv import load_dotenv
from typing import Any, Dict
import os

load_dotenv()


class SnowConnection:

    def __init__(self):
        self.connection_paramters = self._get_connection_parameters_from_env()
        self.session = None

    @staticmethod
    def _get_connection_parameters_from_env() -> Dict[str, Any]:

        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
        return connection_parameters

    def getSession(self):
        if self.session is None:
            self.session = Session.builder.configs(
                self.connection_paramters).create()
            self.session.sql_simplifier_enabled = True
        return self.session
