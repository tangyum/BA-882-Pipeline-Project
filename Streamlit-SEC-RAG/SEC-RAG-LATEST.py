# imports
import streamlit as st
import re
import pandas as pd

from google.cloud import secretmanager
from pinecone import Pinecone
from openai import OpenAI
import os


# setup
project_id = 'ba882-435919'
vector_secret = 'pinecone'
openai_secret = 'openai_token'

version_id = '1'

vector_index = 'sec-rag'
EMBEDDING_MODEL = "text-embedding-3-large"

# secret manager
sm = secretmanager.SecretManagerServiceClient()
vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"
response = sm.access_secret_version(request={"name": vector_name})
pinecone_token = response.payload.data.decode("UTF-8")

openai_name = f"projects/{project_id}/secrets/{openai_secret}/versions/{version_id}"
response = sm.access_secret_version(request={"name": openai_name})
openai_token = response.payload.data.decode("UTF-8")


pc = Pinecone(api_key=pinecone_token)
index = pc.Index(vector_index)


# openai

os.environ['OPENAI_API_KEY'] = openai_token
client = OpenAI()

# Import self_rag code
exec(open("self_rag.py").read())

def get_embedding(text, model="text-embedding-3-large"):
  #  text = text.replace("\n", " ")
  return client.embeddings.create(input = [text], model=model).data[0].embedding

def get_response(query):
  # our RAG Pipeline
  inputs = {'question':query}
  logs = []
  for output in app.stream(inputs):
      for key, value in output.items():
          # Node
          logs.append(f"Node '{key}':")
          # Optional: print full state at each node
          # pprint.pprint(value["keys"], indent=2, width=80, depth=None)
      logs.append("\n---\n")

  return value["generation"], value['documents'], value['results'], value['question'], logs



# Initialize session state
if 'query_run' not in st.session_state:
    st.session_state.query_run = False
if 'results' not in st.session_state:
    st.session_state.results = None
if 'answer' not in st.session_state:
    st.session_state.answer = None
if 'table_contents' not in st.session_state:
    st.session_state.table_contents = {}

# Streamlit UI
st.image("https://www.sec.gov/themes/custom/uswds_sec/dist/img/logos/sec-logo-1x.png")
st.title("SEC RAG - Intelligently Query Financial Statements!")

# Sidebar
st.sidebar.title("Query SEC Filings in our Database")
search_query = st.sidebar.text_area("Input query here")
top_k = st.sidebar.slider("Top K", 1, 50, step=1, value=10)
search_button = st.sidebar.button("Run the RAG pipeline")

# Main query processing
if search_button and search_query.strip():
    with st.spinner("Processing your query..."):
        answer, tables, results, transformed_question, logs = get_response(search_query)
        st.session_state.answer = answer
        st.session_state.results = results
        st.session_state.query_run = True

        # Populate table_contents
        st.session_state.table_contents = {
            f"{r.metadata['source_file']} - {r.metadata['source_sheet']}": r.metadata['markdown_raw']
            for r in results.matches
        }

# Display results
if st.session_state.query_run:
    st.subheader("Transformed Question:")
    st.markdown(f'### {transformed_question}')

    st.subheader("Answer:")
    st.markdown(f'### {st.session_state.answer}')

    # Table selection
    table_options = list(st.session_state.table_contents.keys())
    selected_table = st.selectbox("Select a table to view:", table_options)

    if selected_table:
        st.subheader(f"Selected Table: {selected_table}")
        st.markdown(st.session_state.table_contents[selected_table])
    # st.subheader('Execution Logs')
    # log = '\n'.join(logs)
    # st.markdown(log)


else:
    st.info("Run a query to see results.")