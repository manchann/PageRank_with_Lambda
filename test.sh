#!/bin/bash

concurrency="2"
newline=$'\n'

HOST="https://rclip4sb9a.execute-api.ap-northeast-2.amazonaws.com/default/inference"

jgfunc_url=('https://jgfunc.azurewebsites.net/api/HttpExample/' '?code=tRkE7ZKLwuOWlB4/MDIkfh8a/lblE/fBpHZ1Tpkd6FRPYGfVHwcWsA==')
dedicated_function_url=('https://dedicated-plan-test.azurewebsites.net/api/HttpExample/' '?code=P1TpmaR/iH/t1b0sbyIMFgABnBWhctA4ktX20vf6EvBpnkBVRZ/TYA==')

snack_url='https://rclip4sb9a.execute-api.ap-northeast-2.amazonaws.com/default/inference'

pagerank_url="https://yq7pnajel1.execute-api.us-east-1.amazonaws.com/first/"

bs=1024
start_f=0
start_s=512
end_f=512
end_s=1024
for es in $concurrency; do
  echo $newline'---------------' $es '개 진행중 -----------------'$newline
  curl $pagerank_url$'?bs=1&start=0&end=512'
  curl $pagerank_url$'?bs=1&start=512&end=1024'
done

