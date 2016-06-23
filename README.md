# InfluenceSong
Find user's most influence song from olleh music's raw data set _ 201501 individual research in Kim lab

## Explanation
Find user's most influence song from raw data set.
Using Java and Hadoop.

## Usage
Assume installation of Hadoop.

Run .jar file with hadoop
```
hadooop jar [full name of jar file] [class name] [analysis object file] [result folder]
```

Check result file
```
hadoop fs -text [result folder]/[result filename]
```
or
```
hadoop fs -tail [result folder]/[result filename]
```

## Mapreduce data form
Step1
  Map
    Input : [song_no]|[time_table]|[access_route]|[customer_no]
    Output : ([song_no]@[customer_no], 1)
  Reduce
    Input : ([song_no]@[customer_no], val)
    Output : [song_no]@[customer_no](tab)[songncustomercount]
Step2
  Map
    Input : [song_no]@[customer_no](tab)[songncustomercount]
    Output : ([customer_no], [song_no]=[songncustomercount])
  Reduce
    Input : ([customer_no], [song_no]=[songncustomercount])
    Output : [song_no]@[customer_no](tab)[songncustomercount]/[songincustomer]
Step3
  Map
    Input : [song_no]@[customer_no](tab)[songncustomercount]/[songincustomer]
    Output : ([song_no], [customer_no]=[songncustomercount]/[songincustomer])
  Reduce
    Input : ([song_no], [customer_no]=[songncustomercount]/[songincustomer])
    Output : [customer_no]@[song_no](tab)[tfidf]|[songncustomercount]
    |[songincustomer]|[totalcustomercount]|[customerwholistenthatsong]
Step4
  Map
    Input : [customer_no]@[song_no](tab)[tfidf]|[songncustomercount]
    |[songincustomer]|[totalcustomercount]|[customerwholistenthatsong]
    Output : ([customer_no], [song_no]@[tfidf])
  Reduce
    Input : ([customer_no], [song_no]@[tfidf])
    Output : [customer_no](tab)[influencesong_no]

## References
- Image Quilting for Texture Synthesis and Transfer(Alexei A. Efros, William T. Freeman)
  - link : http://graphics.cs.cmu.edu/people/efros/research/quilting/quilting.pdf
