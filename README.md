# Value at Risk Analysis

[Presentation for this project](https://www.youtube.com/watch?v=zdmgxdJoouU)


In order to run, start a Cluster (prefered EMR cluster).


You need to have `Zeppelin` and `Spark installed`.

* [Spark Installation instruction](http://spark.apache.org/docs/latest/building-spark.html)

* [Zeppelin Installtion instruction](http://zeppelin-project.org/docs/install/install.html)


Now install Git.
```shell
sudo yum install -y git
sudo apt-get install git
```


Clone this repo.
```shell
git clone https://github.com/brahmbhattspandan/MonteCarloData.git
```

Copy the folder `2AXV35H5P` into your zeppeline directory

Refer [Copy Zeppeline Notebook](http://fedulov.website/2015/10/16/export-apache-zeppelin-notebooks/)


Now you need to inport the external jars.

Refer [Import dependency in Zeppelin](http://fedulov.website/2015/10/16/export-apache-zeppelin-notebooks/)


`Make sure that you download the dependency before running spark code`

If you are using first method, then use following code in Zeppeline notebook.

```scala
%dep
z.reset()
z.load("com.github.nscala-time:nscala-time_2.9.1:1.4.0")
z.load("org.apache.commons:commons-math3:3.5")
```

Open the notebook and `run all paragraph` and you will get thr VAR for all the stocks. If you need specific stocks related VAR then remove those files from `/home/hadoop/MonteCarloData/data/stocks/`

You can look at some of the graph related to data here.
[Bonds](https://public.tableau.com/profile/smit.shah#!/vizhome/Bonds/Story1)
[Google](https://public.tableau.com/profile/smit.shah#!/vizhome/Stock-Google-WithFactors/Google)
