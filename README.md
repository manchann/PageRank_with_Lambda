# 두가지 큰 작업을 다룹니다.

## file_write_parallel
file을 동시에 접근 했을 때 file안의 값이 어떤식으로 변화되는지 실험합니다.<br/>
여러 Lambda가 동시에 한 file을 수정해야하기 때문에 검증해볼 필요가 있습니다.

## pagerank_lambda
Lambda에서 s3를 이용하여 file을 가져와 pagerank를 측정해 나가는 워크로드를 구현합니다.