table_name="jg-page-relation"
file_name="jg-page-relation-result"
aws dynamodb scan --table-name ${table_name} --region us-west-2 \
  --output json >${file_name}.json