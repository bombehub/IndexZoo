#profiling
filename=exp5_result.txt
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a3 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {0.0001,0.00025,0.0005,0.00075,0.001};do ./correlation/correlation_test -a3 -i0 -b0 -d1 -u0 -r0 -c1 -t20000000 -q100 -e1 -o0.1 -u1 -s${i} | tee -a ${filename};done;
for i in {0.0001,0.00025,0.0005,0.00075,0.001};do ./correlation/correlation_test -a3 -i1 -b0 -d1 -u0 -r0 -c1 -t20000000 -q100 -e1 -o0.1 -u1 -s${i} | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i0 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {1,5,10,15,20};do ./correlation/correlation_test -a2 -i1 -b0 -d1 -u0 -r0 -c1 -t$((i*1000000)) -q1000000 -e1 -o0.1 | tee -a ${filename};done;
for i in {0.0001,0.00025,0.0005,0.00075,0.001};do ./correlation/correlation_test -a2 -i0 -b0 -d1 -u0 -r0 -c1 -t20000000 -q100 -e1 -o0.1 -u1 -s${i} | tee -a ${filename};done;
for i in {0.0001,0.00025,0.0005,0.00075,0.001};do ./correlation/correlation_test -a2 -i1 -b0 -d1 -u0 -r0 -c1 -t20000000 -q100 -e1 -o0.1 -u1 -s${i} | tee -a ${filename};done;