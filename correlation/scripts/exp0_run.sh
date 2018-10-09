filename=exp0_result.txt
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i0 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i0 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i1 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i1 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {0,0.025,0.05,0.075,0.1,0.2};do ./correlation/correlation_test -a3 -i0 -b0 -d0 -r$i -u0 -c0 -t20000000 -q1000000 -e1 -o0.2 -f8 | tee -a ${filename};done;
for i in {0,0.025,0.05,0.075,0.1,0.2};do ./correlation/correlation_test -a2 -i0 -b0 -d0 -r$i -u0 -c0 -t20000000 -q1000000 -e1 -o0.2 -f8 | tee -a ${filename};done;
for i in {1,2,4,8,10};do ./correlation/correlation_test -a3 -i0 -b0 -d0 -r0 -u0 -c0 -t20000000 -q1000000 -e$i -o0.1 -f8 | tee -a ${filename};done;
for i in {1,2,4,8,10};do ./correlation/correlation_test -a3 -i1 -b0 -d0 -r0 -u0 -c0 -t20000000 -q1000000 -e$i -o0.1 -f8 | tee -a ${filename};done;