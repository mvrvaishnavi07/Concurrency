

1. max request limit is 1000

2. when arrival time is same read is prioritised over delete and write is prioritised over delete

3. if any wrong request name or wrong file number is given, the request is declined right away , not taken into consideration

4. BASIC PRIORITY ORDER : READ > WRITE > DELETE

5. cancellation time : if time >= arrival + timeoout , the req is canceled by user

6. Declining requests at their arrival time + 1 secs if needs to be declined