import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import json
import duckdb
import pandas as pd
from openai import OpenAI
import os
import uuid
import re
import copy
from pinecone import Pinecone, ServerlessSpec

@functions_framework.http
def task(request):
    # parse Excel to markdown
  directory_path = '/content/xlsx/'
  directory_files = os.listdir(directory_path)
  list_of_embedding_dicts = []
  iter = 0
  for workbook in directory_files:
      print(f'Workbook: {workbook}')
      iter +=1
      main_metadata = {}
      xl = pd.ExcelFile(directory_path+workbook)
      sheets = xl.sheet_names
      # extract workbook specific metadata
      sheet = sheets[0]
      df = pd.read_excel(xl,sheet,index_col=0)
      main_metadata = {}
      print(f'{sheet} is first sheet. collecting metadata')
      main_metadata['metadata'] = {}
      main_metadata['metadata']['source_file'] = workbook
      if '10-K' in workbook:
          main_metadata['metadata']['report_type'] = '10-K'
      if '10-Q' in workbook:
          main_metadata['metadata']['report_type'] = '10-Q'
      cover_col = re.search('(\d* Months Ended)', df.to_markdown(), re.IGNORECASE).group(1)
      print(cover_col)
      main_metadata['metadata']['document_period'] = cover_col
      main_metadata['metadata']['company_name'] = df.loc['Entity Registrant Name', cover_col]
      main_metadata['metadata']['ticker'] = df.loc['Trading Symbol', cover_col] # not working as expected (capturing multiple cells), just get from file name
      main_metadata['metadata']['document_end_date'] = df.loc['Document Period End Date', cover_col]

      for i in range(len(sheets)):
          print(sheets[i])
          df = pd.read_excel(xl, sheets[i], index_col=0)
          # extract sheet specific metadata
          embed_dict = copy.deepcopy(main_metadata)  # Create a copy of main_metadata
          embed_dict['id'] = str(uuid.uuid4())
          embed_dict['metadata']['source_sheet'] = sheets[i]
          header = f"""TITLE: {embed_dict['metadata']['company_name']} ({embed_dict['metadata']['ticker']}) {embed_dict['metadata']['report_type']} report for {embed_dict['metadata']['document_end_date']} {embed_dict['metadata']['document_period']} - {sheet} sheet\n"""
          embed_dict['metadata']['header'] = header

          # df to markdown
          md = df.to_markdown()
          md = re.sub('\Wnan\W', '', md)  # remove dubious nan values caused by excel empty cells
          embed_dict['metadata']['markdown_raw'] = md

          list_of_embedding_dicts.append(embed_dict)

  pinecone_token = userdata.get('PINECONE_BU')

  pc = Pinecone(api_key=pinecone_token)

  index_name = 'sec-rag'
  if not pc.has_index(index_name):
      pc.create_index(
          name=index_name,
          dimension=3072, # text-embedding-3-large
          metric="cosine",
          spec=ServerlessSpec(
              cloud='aws', # gcp <- not part of free
              region='us-east-1' # us-central1 <- not part of free
          )
      )


  # embed
  OPENAI_API =  userdata.get('OPENAI_API')

  os.environ['OPENAI_API_KEY'] = OPENAI_API

  from openai import OpenAI
  client = OpenAI()

  def get_embedding(text, model="text-embedding-3-large"):
    #  text = text.replace("\n", " ")
    return client.embeddings.create(input = [text], model=model).data[0].embedding

  copy.deepcopy((list_of_embedding_dicts))
  for record in embeddings:
    try:
      record['values'] = get_embedding(f"{record['metadata']['header']} {record['metadata']['markdown_raw']}")
    except:
      print('exception')
      exception_list.append(record)

  exception_list_pc = []
  def get_byte_length(item):
      return len(json.dumps(item).encode('utf-8'))

  for i in embeddings_3:
    i['metadata']['ticker'] = str(i['metadata']['ticker'])

  index = pc.Index(index_name)

  for i in embeddings_3:
    try: # upsert
      index.upsert(vectors=[i])
    except:
      try: # reducing metadata size (usual problem)
        i['metadata']['markdown_raw'] = i['metadata']['markdown_raw'].replace('-','')
        if len(i['metadata']['markdown_raw']) > 27000:
          i['metadata']['markdown_raw'] = i['metadata']['markdown_raw'][:26999]
          index.upsert(vectors=[i])

      except: # add to exceptions list for pinecone
        print('exception')
        exception_list_pc.append(i)



  len(exception_list_pc)
  return {'response':200}
