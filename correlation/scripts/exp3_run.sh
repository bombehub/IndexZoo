filename=exp3_result.txt
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i0 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i0 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i1 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i1 -b0 -d0 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q100 -e1 -o0.1 -u1 -s0.0001 | tee -a ${filename};done;