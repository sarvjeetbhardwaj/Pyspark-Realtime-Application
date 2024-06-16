import os

os.environ['env'] = 'PROD'
#os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
env = os.environ['env']

appName = 'Pyspark-realtime-application'

current_dir = os.getcwd()

bucket_name = 'pyspark-relatime-app'
src_olap = 'olap'
src_oltp = 'oltp'
