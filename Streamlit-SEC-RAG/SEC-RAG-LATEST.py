
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

def get_embedding(text, model="text-embedding-3-large"):
  #  text = text.replace("\n", " ")
  return client.embeddings.create(input = [text], model=model).data[0].embedding

def get_response(query, retrieved, model='gpt-4o'):
  # our RAG Pipeline

  response = client.chat.completions.create(
    model=model,
    messages=[
        {"role": "system", "content": """Answer the users QUESTION using the DOCUMENTS text below.
                Keep your answer ground in the facts of the DOCUMENTS.
                If the DOCUMENT doesn’t contain the facts to answer the QUESTION return {NONE}"""},
        {"role": "user", "content": f"QUESTION:{query}\n DOCUMENTS{retrieved}"}
    ]
)
  
  return response




############################################## streamlit setup


st.image("https://www.sec.gov/themes/custom/uswds_sec/dist/img/logos/sec-logo-1x.png") # https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS9UovrZvGw3LwsIo3lQ2b-CaPDTVnjbP29Mw&s
st.title("SEC RAG - Intelligently Query Financial Statements!")


################### sidebar

st.sidebar.title("Query SEC Filings in our Database") # add dynamic no. of files in DB

search_query = st.sidebar.text_area("Input query here")

top_k = st.sidebar.slider("Top K", 1, 15, step=1, value=5)

search_button = st.sidebar.button("Run the RAG pipeline")


################### main

# Main action: Handle search
if search_button:
    if search_query.strip():
      with st.spinner('Processing your query...'):
        # Get embedding
        embedding = get_embedding(search_query)

        # search pincone
        results = index.query(
            vector=embedding,
            top_k=top_k,
            include_metadata=True
        )

        # answer the question
        chunks = [r.metadata['header']  + r.metadata['markdown_raw'] for r in results.matches]
        print(results)
        context = "\n".join(chunks)


        response = get_response(search_query, context)
        answer = response.choices[0].message.content
        # Display the results
        st.subheader("Answer:")
        st.markdown(answer)

        # # return the full document from just the first entry - 
        # top_table = f"{results.matches[0]['metadata']['source_file']} - {results.matches[0]['metadata']['source_sheet']}" 
        # table_md = results.matches[0]['metadata']['markdown_raw']
        # st.markdown(f'### {top_table}')

        # Collect all unique tables
        unique_tables = set()
        table_contents = {}

        for result in results.matches:
            source_key = f"{result.metadata['source_file']} - {result.metadata['source_sheet']}"
            unique_tables.add(source_key)
            table_contents[source_key] = result.metadata['markdown_raw']

        # Create table selection dropdown
        selected_table = st.selectbox(
            "Select a table to view:", 
            list(unique_tables)
        )

        # Display selected table's markdown
        if selected_table:
            st.empty()  # Clear previous content
            st.markdown(f"### {selected_table}")
            st.markdown(table_contents[selected_table])

        # # Optional: Add table search functionality
        # search_table = st.text_input("Search within tables")
        # if search_table:
        #     filtered_tables = [
        #         table for table in unique_tables 
        #         if search_table.lower() in table.lower()
        #     ]
        #     if filtered_tables:
        #         selected_table = st.selectbox(
        #             "Filtered Tables:", 
        #             filtered_tables
        #         )
        #         st.markdown(table_contents[selected_table])




    else:
        st.warning("Please enter a search query!")

