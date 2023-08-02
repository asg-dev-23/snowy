import openai
import re
import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
from dotenv import load_dotenv
import os

from utils.prompts import get_system_prompt
from utils.snow_connect import SnowConnection

load_dotenv()

st.title("☃️ Snowy")

openai.api_key = os.getenv("OPENAI_API_KEY")
# openai.api_key = st.secrets.OPENAI_API_KEY

if "messages" not in st.session_state:
    # system prompt includes table information, rules, and prompts the LLM to produce
    # a welcome message to the user.
    st.session_state.messages = [
        {"role": "system", "content": get_system_prompt()}]

# Prompt for user input and save
if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})

# display the existing chat messages
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "results" in message:
            st.dataframe(message["results"])

# If last message is not from assistant, we need to generate a new response
# use GPT-3.5 to generate a response. Instead of displaying the entire response at once,
# use OpenAI's stream parameter to signify that GPT-3.5's response should be sent incrementally
# in chunks via an event stream, and display the chunks as they're received

if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        response = ""
        resp_container = st.empty()
        for delta in openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]}
                      for m in st.session_state.messages],
            stream=True,
        ):
            response += delta.choices[0].delta.get("content", "")
            resp_container.markdown(response)

        # Use a regular expression to search the newly generated response for the
        # SQL markdown syntax that we instructed GPT-3.5 to wrap any SQL queries in.
        # If a match is found, use st.experimental_connection to execute the SQL query
        # against the database we created in Snowflake. Write the result to the app using
        # st.dataframe, and append the result to the associated message in the message history.

        message = {"role": "assistant", "content": response}
        with st.spinner("Executing query..."):
            # Parse the response for a SQL query and execute if available
            sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
            if sql_match:
                query = sql_match.group(1)
                conn = SnowConnection().getSession()  # st.experimental_connection("snowpark")
                try:
                    message["results"] = conn.sql(query).collect()
                    st.dataframe(message["results"])
                except SnowparkSQLException as e:
                    # print(e)
                    error_message = (
                        "You gave me a wrong SQL. FIX The SQL query by searching the schema definition:  \n```sql\n"
                        + query
                        + "\n```\n Error message: \n "
                        + str(e)
                    )
                    st.session_state.messages.append(
                        {"role": "user", "content": error_message})
                    message["error"] = f"Error executing query: {e}"
                conn.close()

        st.session_state.messages.append(message)
