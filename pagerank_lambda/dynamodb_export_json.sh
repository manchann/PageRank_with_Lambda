table_name="jg-pagerank"
file_name="jg-pagerank"
aws dynamodb scan --table-name ${table_name} --region us-west-2 \
  --output json >${file_name}.json